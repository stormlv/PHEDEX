package PHEDEX::File::Download::Circuits::CircuitManager;

use strict;
use warnings;

use base 'PHEDEX::Core::Logging', 'Exporter';
use PHEDEX::Core::Command;
use PHEDEX::Core::Timing;
use PHEDEX::File::Download::Circuits::Circuit;
use PHEDEX::File::Download::Circuits::Constants;
use POE;
use Switch;

my $ownHandles = {
    # OWN
    VERIFY_STATE        =>      'verifyStateConsistency',
    CULL_LIST           =>      'cullBlacklist',
    REQUEST             =>      'requestCircuit',
    REQUEST_REPLY       =>      'handleRequestResponse',
    REQUEST_TIMER       =>      'handleRequestTimeout',
    TEARDOWN            =>      'teardownCircuit',     
};

my $backHandles = {
     # BACKEND
    BACKEND_REQUEST     =>      'backendRequestCircuit',
    BACKEND_TEARDOWN    =>      'backendTeardownCircuit',
};

sub new
{
    my $proto = shift;
    my $class = ref($proto) || $proto;
    
    my %params = (            
            
            # Right now we only support the creation of *one* circuit for each link {(from, to) node pair}
            # Because of this we can safely associate a {PHEDEX_FROM_NODE, PHEDEX_TO_NODE} to each circuit object
            # which makes looking for which circuits are online or in request, easy.
            # We might want to support more than one per link. In this case, the above will need changing  
            CIRCUITS                        => {},          # All circuits in request or established, grouped by link (LINK -> CIRCUIT)
            CIRCUITS_HISTORY                => {},          # All circuits no older than CIRCUIT_HISTORY_DURATION, which are offline, grouped by link then ID (LINK -> ID -> [CIRCUIT1,...])
            CIRCUIT_SCOPE                   =>  'GENERIC',
                                                                 	  	    
	  	    LINKS_BLACKLISTED               => {},          # Links (w/ circuits) which were blacklisted either because requests 
	  	                                                    # failed or because too many transfers failed on them
                                                     
            CIRCUITDIR                      => '',
            
            BLACKLIST_DURATION              => HOUR,        # Time in seconds, after which a circuit will be reconsidered
            CIRCUIT_HISTORY_DURATION        => 6*HOUR,      # Time in seconds after which a circuit will be removed from the OFFLINE hash
            
            MAX_HOURLY_FAILURE_RATE         => 100,         # Maximum of 100 transfers in one hour can fail 

            # Timings of periodic events                                              
            PERIOD_CONSISTENCY_CHECK        => MINUTE,          # Period for event verify_state_consistency  
            PERIOD_BLACKLIST_CULLING        => MINUTE,          # Period for event cull_blacklist      
            
            # Time out counters
            CIRCUIT_REQUEST_TIMEOUT         => 5*MINUTE,        # If we don't get it by then, we'll most likely not get them at all         
            
            # Circuit booking backend options
            BACKEND_TYPE                    => undef,
            BACKEND                         => undef,            
                   
            # POE Session id
            SESSION_ID                      => undef,      
                       
            VERBOSE                         => undef,                                               	
		);
		
    my %args = (@_);
    #   use 'defined' instead of testing on value to allow for arguments which are set to zero.
    map { $args{$_} = defined($args{$_}) ? $args{$_} : $params{$_} } keys %params;
    my $self = $class->SUPER::new(%args);

    # Load circuit booking backend    
    my $backend = $args{BACKEND_TYPE};
    my %backendArgs = %{$args{BACKEND_ARGS}} unless ! defined $args{BACKEND_ARGS};
    eval ("use PHEDEX::File::Download::Circuits::Backend::$backend");
    do { chomp ($@); die "Failed to load backend: $@\n" } if $@;
    $self->{BACKEND} = eval("new PHEDEX::File::Download::Circuits::Backend::$backend(%backendArgs)");
    do { chomp ($@); die "Failed to create backend: $@\n" } if $@;     
 
    bless $self, $class;
    return $self;
}

=pod

# Initialize all POE events (and specifically those related to circuits)

=cut

sub _poe_init
{
    my ($self, $kernel, $session) = @_;
    my $mess = 'CircuitManager->_poe_init';
    
    # Remembering the session ID for when we need to stop and tear down all the circuits
    $self->{SESSION_ID} = $session->ID;
    
    $self->Logmsg("$mess: Initializing all POE events") if ($self->{VERBOSE});
    
    foreach my $key (keys %{$ownHandles}) {
        $kernel->state($ownHandles->{$key}, $self);    
    }
        
    # Share the session with the circuit booking backend as well
    $self->Logmsg("$mess: Initializing all POE events for backend") if ($self->{VERBOSE});
    $self->{BACKEND}->_poe_init($kernel, $session);
      
    # Get the periodic events going 
    $kernel->yield($ownHandles->{VERIFY_STATE}) if (defined $self->{PERIOD_CONSISTENCY_CHECK});
    $kernel->yield($ownHandles->{CULL_LIST}) if (defined $self->{PERIOD_BLACKLIST_CULLING});;
}

# Method used to check if a circuit is either in request or online
# Need to provide $status from the (circuit related) Constants.pm list
# This method returns the circuit if the status matches
sub checkCircuit {
    my ($self, $fromNode, $toNode, $status) = @_;
    return undef if ((!$fromNode || !$toNode || !$status) ||
                     ($status != CIRCUIT_STATUS_ONLINE && $status != CIRCUIT_STATUS_REQUESTING));
    my $linkID = &getLink($fromNode, $toNode);
    my $circuit = $self->{CIRCUITS}{$linkID};    
    return defined $circuit && $circuit->{STATUS} == $status ? $circuit : undef;
}

# Method used to check if we can request a circuit
# - it checks with the backend to see if the link supports a circuit
# - it checks if a request hasn't already been made or that a circuit is not already online
# - it checks if the link isn't blacklisted
sub canRequestCircuit {
    my ($self, $fromNode, $toNode) = @_;             
    my $linkID = &getLink($fromNode, $toNode);
    my $circuit = $self->{CIRCUITS}{$linkID};
    my $mess = "CircuitManager->canRequestCircuit: cannot request another circuit for $linkID";
    
    # A circuit is already requested (or established)
    if (defined $circuit) {
        $self->Logmsg("$mess: One has already been requested");
        return CIRCUIT_ALREADY_REQUESTED;
    }
            
    # Current link is blacklisted           
    my $blacklisted = $self->{LINKS_BLACKLISTED}{$linkID};
    if ($blacklisted) {
        $self->Logmsg("$mess: Link is blacklisted");
        return CIRCUIT_BLACKLISTED;
    }
    
    # Current backend doesn't support circuits
    if (!$self->{BACKEND}->checkLinkSupport($fromNode, $toNode)) {
        $self->Logmsg("$mess: Current booking backend does not allow circuits on this link");
        return CIRCUIT_UNAVAILABLE;
    }
    
    return CIRCUIT_AVAILABLE;
}

# This (recurrent) event is used to ensure consistency between data on disk and data in memory
# If the download agent crashed, these are scenarios that we need to check for:
#   internal data is lost, but file(s) exist in :
#   - circuits/requested
#   - circuits/online
#   - circuits/offline   
sub verifyStateConsistency
{
    my ( $self, $kernel, $session ) = @_[ OBJECT, KERNEL, SESSION];       
    my ($allCircuits, @circuitsRequested, @circuitsOnline, @circuitsOffline);
      
    my $mess = "CircuitManager->$ownHandles->{VERIFY_STATE}";
      
    $self->Logmsg("$mess: enter event") if ($self->{VERBOSE});
    $self->delay_max($kernel, $ownHandles->{VERIFY_STATE}, $self->{PERIOD_CONSISTENCY_CHECK}) if (defined $self->{PERIOD_CONSISTENCY_CHECK});
         
    &getdir($self->{CIRCUITDIR}."/requested", \@circuitsRequested);
    &getdir($self->{CIRCUITDIR}."/online", \@circuitsOnline);
    &getdir($self->{CIRCUITDIR}."/offline", \@circuitsOffline);    
    
    $allCircuits->{'requested'} = \@circuitsRequested;
    $allCircuits->{'online'} = \@circuitsOnline;
    $allCircuits->{'offline'} = \@circuitsOffline;

    my $timeNow = &mytimeofday();
    
    foreach my $tag (keys %{$allCircuits}) {        
        
        # Skip if there are no files in one of the 3 folders
        if (!scalar @{$allCircuits->{$tag}}) {
            $self->Logmsg("$mess: No files found in /$tag") if ($self->{VERBOSE}); 
            next;
        }
        
        foreach my $file (@{$allCircuits->{$tag}}) {            
            my $path = $self->{CIRCUITDIR}.'/'.$tag.'/'.$file;
            $self->Logmsg("$mess: Now handling $path") if ($self->{VERBOSE});
                    
            # Attempt to open circuit                   
            my ($circuit, $code) = &openCircuit($path);            
            my $circuitOK = $code == CIRCUIT_OK;        
            
            # Remove the state file if the read didn't return OK 
            if (!$circuitOK) {
                $self->Logmsg("$mess: Removing invalid circuit file $path");
                unlink $path;
                next;
            }
            
            # Check to see if the circuit was saved in the proper place
            # If not, remove the link and force a resave (should put it in the proper place after this)
            if (($circuit->{STATUS} == CIRCUIT_STATUS_REQUESTING && $tag ne 'requested') ||
                ($circuit->{STATUS} == CIRCUIT_STATUS_ONLINE && $tag ne 'online') ||
                ($circuit->{STATUS} == CIRCUIT_STATUS_OFFLINE && $tag ne 'offline')) {
                    $self->Logmsg("$mess: Found circuit in incorrect folder. Removing and resaving...");
                    unlink $path;              
                    $circuit->saveState();       
            }                
            
            my $linkName = $circuit->getLinkName();
            
            # The following three IFs could very well have been condensed into one, but
            # I wanted to provide custom debug messages whenver we skipped them
            
            # If the scope doesn't match             
            if ($self->{CIRCUIT_SCOPE} ne $circuit->{SCOPE}) {
                $self->Logmsg("$mess: Skipping circuit since its scope don't match ($circuit->{SCOPE} vs $self->{CIRCUIT_SCOPE})")  if ($self->{VERBOSE});
                next;
            }
            
            # If the backend doesn't match the one we have here, skip it            
            if ($circuit->{BOOKING_BACKEND} ne $self->{BACKEND_TYPE}) {
                $self->Logmsg("$mess: Skipping circuit due to different backend used ($circuit->{BOOKING_BACKEND} vs $self->{BACKEND_TYPE})") if ($self->{VERBOSE});
                next;
            }
            
            # If the backend no longer supports circuits on those links, skip them as well
            if (! $self->{BACKEND}->checkLinkSupport($circuit->{PHEDEX_FROM_NODE}, $circuit->{PHEDEX_TO_NODE})) {
                $self->Logmsg("$mess: Skipping circuit since the backend no longer supports creation of circuits on $linkName") if ($self->{VERBOSE});
                next;
            }

            my $inMemoryCircuit;
            
            # Attempt to retrieve the circuit if it's in memory
            switch ($tag) {
                case 'offline' {
                    my $offlineCircuits = $self->{CIRCUITS_OFFLINE}{$linkName};
                    $inMemoryCircuit = $offlineCircuits->{$circuit->{ID}} if (defined $offlineCircuits && defined $offlineCircuits->{$circuit->{ID}});
                }
                else {
                    $inMemoryCircuit = $self->{CIRCUITS}{$linkName};     
                }
            };
            
            # Skip this one if we found an identical circuit in memory
            if ($circuit->compareCircuits($inMemoryCircuit)) {
                $self->Logmsg("$mess: Skipping identical in-memory circuit") if ($self->{VERBOSE});
                next;
            } 
            
            # If, for the same link, the info differs between on disk and in memory,
            # yet the scope of the circuit is the same as the one for the CM
            # remove the one on disk and force a resave for the one in memory
            if (defined $inMemoryCircuit) {
                 $self->Logmsg("$mess: Removing similar circuit on disk and forcing resave of the one in memory");
                 unlink $path;              
                 $inMemoryCircuit->saveState();
                 next;
            }

            # If we get to here it means that we didn't find anything in memory pertaining to a given link                    
                        
            switch ($tag) {
                case 'requested' {                    
                    # This is a bit tricky to handle.
                    #   1) The circuit could still be 'in request'. If the booking agent died as well 
                    #      then the circuit could be created and not know about it :|
                    #   2) The circuit might be online by now
                    # What we do now is flag the circuit as offline, then have it in the offline thing for historical purposes                
                    # TODO : Another solution would be to wait a bit of time then attempt to 'teardown' the circuit
                    # This would ensure that everything is 'clean' after this method
                    unlink $path;
                    $circuit->registerRequestFailure('Failure to restore request from disk');                    
                    $circuit->saveState();
                }
                case 'online' {
                    # Skip circuit if the link is currently blacklisted                    
                    if (defined $self->{LINKS_BLACKLISTED}{$linkName}) {
                        $self->Logmsg("$mess: Skipping circuit since $linkName is currently blacklisted");
                        # We're not going to remove the file. It might be useful once the blacklist timer expires
                        next;
                    }
                                    
                    # Now there are two cases:
                    if (! defined $circuit->{LIFETIME} ||                                                                           # If the loaded circuit has a defined Lifetime parameter 
                        (defined $circuit->{LIFETIME} && ! $circuit->isExpired())) {                                                # and it's not expired
                        
                        # Use the circuit
                        $self->Logmsg("$mess: Found established circuit $linkName. Using it");
                        $self->{CIRCUITS}{$linkName} = $circuit;     
                                      
                        if (defined $circuit->{LIFETIME}) {                             
                            my $delay = $circuit->getExpirationTime() - &mytimeofday();
                            $self->Logmsg("$mess: Established circuit has lifetime defined. Starting timer for $delay");
                            $kernel->delay_add($ownHandles->{TEARDOWN}, $delay, $circuit);
                        }
                                
                    } else {                                                                                                        # Else we attempt to tear it down
                        $self->Logmsg("$mess: Attempting to teardown expired circuit $linkName");
                        $kernel->post($session, $ownHandles->{TEARDOWN}, $circuit);                        
                    }                                              
                }                  
                case 'offline' {
                    # Only add the most recent (up to CIRCUIT_HISTORY_DURATION) circuit history
                    if ($circuit->{LAST_STATUS_CHANGE} > $timeNow - $self->{CIRCUIT_HISTORY_DURATION}) {
                        $self->Logmsg("$mess: Found offline circuit. Adding it to history");
                        $self->{CIRCUITS_HISTORY}{$linkName}{$circuit->{ID}} = $circuit;                        
                    }
                }
            }
        }
    }
}

# Circuits can be blacklisted for two reasons
# 1. A circuit request previously failed
#   - to prevent successive multiple retries to the same IDC, we temporarily blacklist that particular link
# 2. Multiple files in a job failed while being transferred on the circuit
#   - if transfers fail because of a circuit error, by default PhEDEx will retry transfers on the same link
#   we temporarily blacklist that particular link and PhEDEx will retry on a "standard" link instead
sub cullBlacklist {
    my ( $self, $kernel, $session ) = @_[ OBJECT, KERNEL, SESSION];    
    my (@circuitsRequested, @circuitsEstablished);
    
    my $mess = "CircuitManager->$ownHandles->{CULL_LIST}";  
    
    $self->Logmsg("$mess: Enter event") if ($self->{VERBOSE});    
    $self->delay_max($kernel, $ownHandles->{CULL_LIST}, $self->{PERIOD_BLACKLIST_CULLING}) if (defined $self->{PERIOD_BLACKLIST_CULLING});;
    
    foreach my $linkName (keys %{$self->{LINKS_BLACKLISTED}}) {
        my $listData = $self->{LINKS_BLACKLISTED}{$linkName};
        my $time = $listData->[0];
        my $details = $listData->[1];
        
        if ($time + $self->{BLACKLIST_DURATION} < &mytimeofday()) {
            $self->Logmsg("$mess: Removing $linkName from blacklist after $self->{BLACKLIST_DURATION} seconds");
            delete  $self->{LINKS_BLACKLISTED}{$linkName};
        }
    }
}

# This routine is called by the CircuitAgent when a transfer fails
# If too many transfer fail, it will teardown and blacklist the circuit
sub transferFailed {
    my ($self, $circuit, $code) = @_;
    
    my $mess = "CircuitManager->transferFailed";
    
    if (!defined $circuit || !defined $code) {
        $self->Logmsg("$mess: Circuit or code not defined");
        return;
    }
    
    if ($circuit->{STATUS} != CIRCUIT_STATUS_ONLINE) {
        $self->Logmsg("$mess: Can't do anything with this circuit");
        return;
    }
    
    # Tell the circuit that a transfer failed on it    
    $circuit->registerTransferFailure($code);
    
    my $transferFailures = $circuit->getFailedTransfers();  
    my $lastHourFails;
    my $now = &mytimeofday();
    
    foreach my $fails (@{$transferFailures}) {
        $lastHourFails++ if ($fails->[0] > $now - HOUR);            
    }  
        
    my $linkName = $circuit->getLinkName();
    
    if ($lastHourFails > $self->{MAX_HOURLY_FAILURE_RATE}) {
        $self->Logmsg("$mess: Blacklisting $linkName due to too many transfer failures");
        
        # Blacklist the circuit
        $self->{LINKS_BLACKLISTED}{$linkName} = [$now, $transferFailures];
        
        # Tear it down
        POE::Kernel->call($self->{SESSION_ID}, $ownHandles->{TEARDOWN}, $circuit);
    }
}

sub requestCircuit {    
    my ( $self, $kernel, $session, $from_node, $to_node, $lifetime, $bandwidth) = @_[ OBJECT, KERNEL, SESSION, ARG0, ARG1, ARG2, ARG3 ];
    
    my $mess = "CircuitManager->$ownHandles->{REQUEST}"; 
    
    # Check if link is defined            
    if (!defined $from_node || !defined $to_node) {
        $self->Logmsg("$mess: Provided link is invalid - will not attempt a circuit request");
        return CIRCUIT_INVALID;
    }
    
    # Check with circuit booking backend to see if the nodes actually support circuits
    if (! $self->{BACKEND}->checkLinkSupport($from_node, $to_node)) {
        $self->Logmsg("$mess: Provided link does not support circuits");
        return CIRCUIT_UNAVAILABLE;
    }
       
    my $linkName = &getLink($from_node, $to_node);
    
    # This check theoretically shouldn't really be necessary    
    if ($self->{CIRCUITS}{$linkName}) {
        $self->Logmsg("$mess: Skipping request for $linkName since there is already a request/circuit ongoing");
        return;
    }
          
    $self->Logmsg("$mess: Attempting to request a circuit for link $linkName");   
    defined $lifetime ? $self->Logmsg("$mess: Lifetime for link $linkName is $lifetime seconds") :
                        $self->Logmsg("$mess: Lifetime for link $linkName is the maximum allowable by IDC");
           
    # Create the circuit object
    my $circuit = PHEDEX::File::Download::Circuits::Circuit->new(BOOKING_BACKEND => $self->{BACKEND_TYPE},
                                                                 STATE_FOLDER => $self->{CIRCUITDIR},
                                                                 SCOPE => $self->{CIRCUIT_SCOPE},
                                                                 VERBOSE => $self->{VERBOSE});
    $circuit->setNodes($from_node, $to_node);
    $circuit->{CIRCUIT_REQUEST_TIMEOUT} = $self->{CIRCUIT_REQUEST_TIMEOUT};
    
    $self->Logmsg("$mess: Created circuit for link $linkName (Circuit ID = $circuit->{ID})");
        
    # Switch from the 'offline' to 'requesting' state, save to disk and store this in our memory
    eval {                                                                                                            
        $circuit->registerRequest($self->{BACKEND_TYPE}, $lifetime, $bandwidth);   
        $self->{CIRCUITS}{$linkName} = $circuit;
        $circuit->saveState();
        
        # Start the watchdog in case the request times out
        $kernel->delay_add($ownHandles->{REQUEST_TIMER}, $circuit->{CIRCUIT_REQUEST_TIMEOUT}, $circuit);
        $kernel->post($session, $backHandles->{BACKEND_REQUEST}, $circuit, $ownHandles->{REQUEST_REPLY});
    };

}

# This method is called when a circuit request fails.
# This is either because the request itself failed (got a reply and an error code) or
# the request timed out. In either case, this is obviously bad and what needs doing
# is the same in both cases
sub handleRequestFailure {
    my ($self, $circuit, $code) = @_;
    
    my $mess = "CircuitManager->handleRequestFailure";
    
    if (!defined $circuit) {
        $self->Logmsg("$mess: No circuit was provided");
        return;        
    }    
    
    my $linkName = $circuit->getLinkName();
    
    if ($circuit->{STATUS} != CIRCUIT_STATUS_REQUESTING) {
        $self->Logmsg("$mess: Can't do anything with this circuit");
        return;
    }
    
    eval {
        $self->Logmsg("$mess: Updating internal data");
        # Remove the state that was saved to disk
        $circuit->removeState();
        
        # Remove from hash of all circuits, then add it to the historical list
        delete $self->{CIRCUITS}{$linkName};
        $self->{CIRCUITS_HISTORY}{$linkName}{$circuit->{ID}} = $circuit;
        
        my $now = &mytimeofday();
        
        # Update circuit object internal data as well
        $circuit->registerRequestFailure($code);
        
        # Blacklist this link 
        # This needs to be done *after* we register the failure with the circuit
        $self->{LINKS_BLACKLISTED}{$linkName} = [$now, $circuit->getFailedRequest()];       
       
        $circuit->saveState();
    }
        
}

sub handleRequestResponse {
    my ($self, $kernel, $session, $circuit, $return, $code) = @_[ OBJECT, KERNEL, SESSION, ARG0, ARG1, ARG2 ];

    my $mess = "CircuitManager->$ownHandles->{REQUEST_REPLY}"; 
            
    if (!defined $circuit || !defined $code) {
        $self->Logmsg("$mess: Circuit or code not defined");
        return;        
    }  
    
    my $linkName = $circuit->getLinkName();
    
    my $blaCircuit = $self->{CIRCUITS}{$linkName};
    
    if ($circuit->{STATUS} != CIRCUIT_STATUS_REQUESTING) {
        $self->Logmsg("$mess: Can't do anything with this circuit");
        return;
    }
    
    # If the circuit request failed, call the method handling request failures
    if ($code < 0) {
        $self->handleRequestFailure($circuit, $code);
        $self->Logmsg("$mess: Circuit request failed for $linkName");
        return;        
    } 
    
    # If the circuit request succeeded ... yay   
    $self->Logmsg("$mess: Circuit request succeeded for $linkName"); 
    $circuit->removeState(); 
    $circuit->registerEstablished($return->{FROM_IP}, $return->{TO_IP}, $return->{BANDWIDTH});    
    $circuit->saveState();            
      
    $self->{CIRCUITS}{$linkName} = $circuit;
    if (defined $circuit->{LIFETIME}) {
        $self->Logmsg("$mess: Circuit has an expiration date. Starting countdown to teardown");
        $kernel->delay_add('teardownCircuit', $circuit->{LIFETIME}, $circuit);
    }
}

sub handleRequestTimeout {
    my ($self, $kernel, $session, $circuit) = @_[ OBJECT, KERNEL, SESSION, ARG0];
    
    my $mess = "CircuitManager->$ownHandles->{REQUEST_TIMER}";
    
    if (!defined $circuit) {
        $self->Logmsg("$mess: something went horribly wrong... Didn't receive a circuit back");
         return;        
    } 
    
    $self->Logmsg("$mess: Timer has expired. Request will be ignored and link blacklisted");
    $self->handleRequestFailure($circuit, CIRCUIT_REQUEST_FAILED_TIMEDOUT);   
}

sub teardownCircuit {
    my ($self, $kernel, $session, $circuit) = @_[ OBJECT, KERNEL, SESSION, ARG0];
    
    my $mess = "CircuitManager->$ownHandles->{TEARDOWN}";
    
    if (!defined $circuit) {
        $self->Logmsg("$mess: something went horribly wrong... Didn't receive a circuit back");
        return;        
    } 
         
    my $linkName = $circuit->getLinkName();
    $self->Logmsg("$mess: Updating states for link $linkName");
    
    eval {
        $circuit->removeState();
            
        # Remove from hash of all circuits, then add it to the historical list
        delete $self->{CIRCUITS}{$linkName};
        $self->{CIRCUITS_HISTORY}{$linkName}{$circuit->{ID}} = $circuit;
            
        # Update circuit object data  
        $circuit->registerTakeDown();
                    
        # Potentiall save the new state for debug purposes
        $circuit->saveState();
    };
    
    $self->Logmsg("$mess: Calling teardown for $linkName");
    # Call backend to take down this circuit
    $kernel->post($session, $backHandles->{BACKEND_TEARDOWN}, $circuit);    
}

# Cancels all requests in progress and tears down all the circuits that have been established
sub teardownAll {
    my ($self) = @_;
    
    my $mess = "CircuitManager->teardownAll";
    
    $self->Logmsg("$mess: Cleaning out all circuits");
    
    foreach my $circuit (values %{$self->{CIRCUITS}}) {
        my $backend = $self->{BACKEND};        
        switch ($circuit->{STATUS}) {
            case CIRCUIT_STATUS_REQUESTING {
                # TODO: Check and see if you can cancel requests
                # $backend->cancel_request($circuit);
            }
            case CIRCUIT_STATUS_ONLINE {
                $self->Logmsg("$mess: Tearing down circuit for link $circuit->getLinkName()");
                POE::Kernel->call($self->{SESSION_ID}, $ownHandles->{TEARDOWN}, $circuit);
            }          
        }    
    }
}

## These two methods were 'borrowed' from PHEDEX::File::Download::Agent

# schedule $event to occur AT MOST $maxdelta seconds into the future.
# if the event is already scheduled to arrive before that time,
# nothing is done.  returns the timestamp of the next event
sub delay_max
{
    my ($self, $kernel, $event, $maxdelta) = @_;
    my $now = &mytimeofday();
    my $id = $self->{ALARMS}->{$event}->{ID};
    my $next = $kernel->alarm_adjust($id, 0);
    if (!$next) {
	$next = $now + $maxdelta;
	$id = $kernel->alarm_set($event, $next);
    } elsif ($next - $now > $maxdelta) {
	$next = $kernel->alarm_adjust($id, $now - $next + $maxdelta);
    }
    $self->{ALARMS}->{$event} = { ID => $id, NEXT => $next };
    return $next;
}

# return the timestamp of the next scheduled $event (must be set using delay_max())
# returns undef if there is no event scheduled.
sub next_event_time
{
    my ($self, $event) = @_;
    return $self->{ALARMS}->{$event}->{NEXT};
}

1;