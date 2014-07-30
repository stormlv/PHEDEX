package PHEDEX::File::Download::Circuits::CircuitManager;

use strict;
use warnings;

use base 'PHEDEX::Core::Logging', 'Exporter';
use List::Util qw(min);
use PHEDEX::Core::Command;
use PHEDEX::Core::Timing;
use PHEDEX::File::Download::Circuits::Circuit;
use PHEDEX::File::Download::Circuits::Constants;
use POE;
use Switch;

my $ownHandles = {
    HANDLE_TIMER        =>      'handleTimer',
    REQUEST             =>      'requestCircuit',
    REQUEST_REPLY       =>      'handleRequestResponse',
    VERIFY_STATE        =>      'verifyStateConsistency',   
};

my $backHandles = {
    BACKEND_REQUEST     =>      'backendRequestCircuit',
    BACKEND_TEARDOWN    =>      'backendTeardownCircuit',
};


# Right now we only support the creation of *one* circuit for each link {(from, to) node pair}
# This assumption implies that each {(from, to) node pair} is unique  
sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    
    my %params = (            
                                
            # Main circuit related parameters                        
            CIRCUITS                        => {},          # All circuits in request or established, grouped by link (LINK -> CIRCUIT)
            CIRCUITDIR                      => '',          # Default location to place circuit state files
            SCOPE                   =>  'GENERIC',          # NOT USED atm

            # Circuit booking backend options
            BACKEND_TYPE                    => 'Dummy',
            BACKEND                         => undef,            
                        
            # Parameters related to circuit history
            CIRCUITS_HISTORY                => {},          # Last MAX_HISTORY_SIZE circuits, which are offline, grouped by link then ID (LINK -> ID -> [CIRCUIT1,...])
            CIRCUITS_HISTORY_QUEUE          => [],          # Queue used to keep track of previously active circuits (now in 'offline' mode)  
            MAX_HISTORY_SIZE                => 1000,         # Keep the last xx circuits in memory
            SYNC_HISTORY_FOLDER             => 0,           # If this is set, it will also remove circuits from 'offline' folder           
            
            
            # Parameters related to blacklisting circuist                                                                 	  	    
	  	    LINKS_BLACKLISTED               => {},          # All links currently blacklisted from creating circuits                                                                          
            BLACKLIST_DURATION              => HOUR,        # Time in seconds, after which a circuit will be reconsidered                       
            MAX_HOURLY_FAILURE_RATE         => 100,         # Maximum of 100 transfers in one hour can fail 

            # Parameters related to various timings
            PERIOD_CONSISTENCY_CHECK        => MINUTE,      # Period for event verify_state_consistency  
            CIRCUIT_REQUEST_TIMEOUT         => 5 * MINUTE,  # If we don't get it by then, we'll most likely not get them at all                     
                   
            # POE related stuff
            SESSION_ID                      => undef,
            DELAYS                          => undef,      
                       
            VERBOSE                         => 0,           
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
    my $msg = 'CircuitManager->_poe_init';
    
    # Remembering the session ID for when we need to stop and tear down all the circuits
    $self->{SESSION_ID} = $session->ID;
    
    $self->Logmsg("$msg: Initializing all POE events") if ($self->{VERBOSE});
    
    foreach my $key (keys %{$ownHandles}) {
        $kernel->state($ownHandles->{$key}, $self);    
    }
        
    # Share the session with the circuit booking backend as well
    $self->Logmsg("$msg: Initializing all POE events for backend") if ($self->{VERBOSE});
    $self->{BACKEND}->_poe_init($kernel, $session);
      
    # Get the periodic events going 
    $kernel->yield($ownHandles->{VERIFY_STATE}) if (defined $self->{PERIOD_CONSISTENCY_CHECK});
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
    my $msg = "CircuitManager->canRequestCircuit: cannot request another circuit for $linkID";
    
    # A circuit is already requested (or established)
    if (defined $circuit) {
        $self->Logmsg("$msg: One has already been requested");
        return CIRCUIT_ALREADY_REQUESTED;
    }
            
    # Current link is blacklisted           
    my $blacklisted = $self->{LINKS_BLACKLISTED}{$linkID};
    if ($blacklisted) {
        $self->Logmsg("$msg: Link is blacklisted");
        return CIRCUIT_BLACKLISTED;
    }
    
    # Current backend doesn't support circuits
    if (!$self->{BACKEND}->checkLinkSupport($fromNode, $toNode)) {
        $self->Logmsg("$msg: Current booking backend does not allow circuits on this link");
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
      
    my $msg = "CircuitManager->$ownHandles->{VERIFY_STATE}";
      
    $self->Logmsg("$msg: enter event") if ($self->{VERBOSE});
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
            $self->Logmsg("$msg: No files found in /$tag") if ($self->{VERBOSE}); 
            next;
        }
        
        foreach my $file (@{$allCircuits->{$tag}}) {            
            my $path = $self->{CIRCUITDIR}.'/'.$tag.'/'.$file;
            $self->Logmsg("$msg: Now handling $path") if ($self->{VERBOSE});
                    
            # Attempt to open circuit                   
            my ($circuit, $code) = &openCircuit($path);            
            my $circuitOK = $code == CIRCUIT_OK;        
            
            # Remove the state file if the read didn't return OK 
            if (!$circuitOK) {
                $self->Logmsg("$msg: Removing invalid circuit file $path");
                unlink $path;
                next;
            }
            
            # Check to see if the circuit was saved in the proper place
            # If not, remove the link and force a resave (should put it in the proper place after this)
            if (($circuit->{STATUS} == CIRCUIT_STATUS_REQUESTING && $tag ne 'requested') ||
                ($circuit->{STATUS} == CIRCUIT_STATUS_ONLINE && $tag ne 'online') ||
                ($circuit->{STATUS} == CIRCUIT_STATUS_OFFLINE && $tag ne 'offline')) {
                    $self->Logmsg("$msg: Found circuit in incorrect folder. Removing and resaving...");
                    unlink $path;              
                    $circuit->saveState();       
            }                
            
            my $linkName = $circuit->getLinkName();
            
            # The following three IFs could very well have been condensed into one, but
            # I wanted to provide custom debug messages whenver we skipped them
            
            # If the scope doesn't match             
            if ($self->{SCOPE} ne $circuit->{SCOPE}) {
                $self->Logmsg("$msg: Skipping circuit since its scope don't match ($circuit->{SCOPE} vs $self->{SCOPE})")  if ($self->{VERBOSE});
                next;
            }
            
            # If the backend doesn't match the one we have here, skip it            
            if ($circuit->{BOOKING_BACKEND} ne $self->{BACKEND_TYPE}) {
                $self->Logmsg("$msg: Skipping circuit due to different backend used ($circuit->{BOOKING_BACKEND} vs $self->{BACKEND_TYPE})") if ($self->{VERBOSE});
                next;
            }
            
            # If the backend no longer supports circuits on those links, skip them as well
            if (! $self->{BACKEND}->checkLinkSupport($circuit->{PHEDEX_FROM_NODE}, $circuit->{PHEDEX_TO_NODE})) {
                $self->Logmsg("$msg: Skipping circuit since the backend no longer supports creation of circuits on $linkName") if ($self->{VERBOSE});
                next;
            }

            my $inMemoryCircuit;
            
            # Attempt to retrieve the circuit if it's in memory
            switch ($tag) {
                case 'offline' {
                    my $offlineCircuits = $self->{CIRCUITS_HISTORY}{$linkName};
                    $inMemoryCircuit = $offlineCircuits->{$circuit->{ID}} if (defined $offlineCircuits && defined $offlineCircuits->{$circuit->{ID}});
                }
                else {
                    $inMemoryCircuit = $self->{CIRCUITS}{$linkName};     
                }
            };
            
            # Skip this one if we found an identical circuit in memory
            if ($circuit->compareCircuits($inMemoryCircuit)) {
                $self->Logmsg("$msg: Skipping identical in-memory circuit") if ($self->{VERBOSE});
                next;
            } 
            
            # If, for the same link, the info differs between on disk and in memory,
            # yet the scope of the circuit is the same as the one for the CM
            # remove the one on disk and force a resave for the one in memory
            if (defined $inMemoryCircuit) {
                 $self->Logmsg("$msg: Removing similar circuit on disk and forcing resave of the one in memory");
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
                        $self->Logmsg("$msg: Skipping circuit since $linkName is currently blacklisted");
                        # We're not going to remove the file. It might be useful once the blacklist timer expires
                        next;
                    }
                                    
                    # Now there are two cases:
                    if (! defined $circuit->{LIFETIME} ||                                                                           # If the loaded circuit has a defined Lifetime parameter 
                        (defined $circuit->{LIFETIME} && ! $circuit->isExpired())) {                                                # and it's not expired
                        
                        # Use the circuit
                        $self->Logmsg("$msg: Found established circuit $linkName. Using it");
                        $self->{CIRCUITS}{$linkName} = $circuit;     
                                      
                        if (defined $circuit->{LIFETIME}) {                             
                            my $delay = $circuit->getExpirationTime() - &mytimeofday();                            
                            next if $delay < 0;                            
                            $self->Logmsg("$msg: Established circuit has lifetime defined. Starting timer for $delay");
                            $self->delayAdd($kernel, $ownHandles->{HANDLE_TIMER}, $delay, CIRCUIT_TIMER_TEARDOWN, $circuit);
                        }
                                
                    } else {                                                                                                        # Else we attempt to tear it down
                        $self->Logmsg("$msg: Attempting to teardown expired circuit $linkName");
                        $self->handleCircuitTeardown($kernel, $session, $circuit);                      
                    }                                              
                }                  
                case 'offline' {
                    # Don't add the circuit if the history is full and circuit is older than the oldest that we currently have on record
                    my $oldestCircuit = $self->{CIRCUITS_HISTORY_QUEUE}->[0];
                    if (scalar @{$self->{CIRCUITS_HISTORY_QUEUE}} < $self->{MAX_HISTORY_SIZE} ||
                        !defined $oldestCircuit || $circuit->{LAST_STATUS_CHANGE} > $oldestCircuit->{LAST_STATUS_CHANGE}) {
                        $self->Logmsg("$msg: Found offline circuit. Adding it to history");
                        $self->addCircuitToHistory($circuit);                                               
                    }
                }
            }
        }
    }
}

# Adds a circuit to CIRCUITS_HISTORY
sub addCircuitToHistory {
    my ($self, $circuit) = @_;
    
    my $msg = "CircuitManager->addCircuitToHistory"; 
    
    if (! defined $circuit) {
        $self->Logmsg("$msg: Invalid circuit provided");
        return;
    }
    
    if (scalar @{$self->{CIRCUITS_HISTORY_QUEUE}} >= $self->{MAX_HISTORY_SIZE}) {
        eval {            
            # Remove oldest circuit from history           
            my $oldestCircuit = shift @{$self->{CIRCUITS_HISTORY_QUEUE}};
            $self->Logmsg("$msg: Removing oldest circuit from history ($oldestCircuit->{ID})") if $self->{VERBOSE};
            delete $self->{CIRCUITS_HISTORY}{$oldestCircuit->getLinkName()}{$oldestCircuit->{ID}};
            $oldestCircuit->removeState() if ($self->{SYNC_HISTORY_FOLDER});
        }
    } 
    
    $self->Logmsg("$msg: Adding circuit ($circuit->{ID}) to history");
    
    # Add to history
    eval {
        push @{$self->{CIRCUITS_HISTORY_QUEUE}}, $circuit;
        $self->{CIRCUITS_HISTORY}{$circuit->getLinkName()}{$circuit->{ID}} = $circuit; 
    }         
}

# Blacklists a link and starts a timer to unblacklist it after BLACKLIST_DURATION
sub addLinkToBlacklist {
    my ($self, $circuit, $fault, $delay) = @_;
    
    my $msg = "CircuitManager->addLinkToBlacklist"; 
    
    if (! defined $circuit) {
        $self->Logmsg("$msg: Invalid circuit provided");
        return;
    }
       
    my $linkName = $circuit->getLinkName();
    $delay = $self->{BLACKLIST_DURATION} if ! defined $delay;
    
    $self->Logmsg("$msg: Adding link ($linkName) to history. It will be removed after $delay seconds");
    
    $self->{LINKS_BLACKLISTED}{$linkName} = $fault;
    $self->delayAdd($poe_kernel, $ownHandles->{HANDLE_TIMER}, $delay, CIRCUIT_TIMER_BLACKLIST, $circuit);
}

# This routine is called by the CircuitAgent when a transfer fails
# If too many transfer fail, it will teardown and blacklist the circuit
sub transferFailed {
    my ($self, $circuit, $task) = @_;
    
    my $msg = "CircuitManager->transferFailed";
    
    if (!defined $circuit || !defined $task) {
        $self->Logmsg("$msg: Circuit or code not defined");
        return;
    }
    
    if ($circuit->{STATUS} != CIRCUIT_STATUS_ONLINE) {
        $self->Logmsg("$msg: Can't do anything with this circuit");
        return;
    }
    
    # Tell the circuit that a transfer failed on it    
    $circuit->registerTransferFailure($task);
    
    my $transferFailures = $circuit->getFailedTransfers();  
    my $lastHourFails;
    my $now = &mytimeofday();
    
    foreach my $fails (@{$transferFailures}) {
        $lastHourFails++ if ($fails->[0] > $now - HOUR);            
    }  
        
    my $linkName = $circuit->getLinkName();
    
    if ($lastHourFails > $self->{MAX_HOURLY_FAILURE_RATE}) {
        $self->Logmsg("$msg: Blacklisting $linkName due to too many transfer failures");
        
        # Blacklist the circuit
        $self->addLinkToBlacklist($circuit, CIRCUIT_TRANSFERS_FAILED);
                
        # Tear it down
        $self->handleCircuitTeardown($poe_kernel, $poe_kernel->ID_id_to_session($self->{SESSION_ID}), $circuit);          
    }
}

sub requestCircuit {    
    my ( $self, $kernel, $session, $from_node, $to_node, $lifetime, $bandwidth) = @_[ OBJECT, KERNEL, SESSION, ARG0, ARG1, ARG2, ARG3 ];
    
    my $msg = "CircuitManager->$ownHandles->{REQUEST}"; 
    
    # Check if link is defined            
    if (!defined $from_node || !defined $to_node) {
        $self->Logmsg("$msg: Provided link is invalid - will not attempt a circuit request");
        return CIRCUIT_INVALID;
    }
    
    # Check with circuit booking backend to see if the nodes actually support circuits
    if (! $self->{BACKEND}->checkLinkSupport($from_node, $to_node)) {
        $self->Logmsg("$msg: Provided link does not support circuits");
        return CIRCUIT_UNAVAILABLE;
    }
       
    my $linkName = &getLink($from_node, $to_node);
       
    if ($self->{CIRCUITS}{$linkName}) {
        $self->Logmsg("$msg: Skipping request for $linkName since there is already a request/circuit ongoing");
        return;
    }
    
    if ($self->{LINKS_BLACKLISTED}{$linkName}) {
        $self->Logmsg("$msg: Skipping request for $linkName since it is currently blacklisted");
        return;
    }
          
    $self->Logmsg("$msg: Attempting to request a circuit for link $linkName");   
    defined $lifetime ? $self->Logmsg("$msg: Lifetime for link $linkName is $lifetime seconds") :
                        $self->Logmsg("$msg: Lifetime for link $linkName is the maximum allowable by IDC");
           
    # Create the circuit object
    my $circuit = PHEDEX::File::Download::Circuits::Circuit->new(BOOKING_BACKEND => $self->{BACKEND_TYPE},
                                                                 CIRCUITDIR => $self->{CIRCUITDIR},
                                                                 SCOPE => $self->{SCOPE},
                                                                 VERBOSE => $self->{VERBOSE});
    $circuit->setNodes($from_node, $to_node);
    $circuit->{CIRCUIT_REQUEST_TIMEOUT} = $self->{CIRCUIT_REQUEST_TIMEOUT};
    
    $self->Logmsg("$msg: Created circuit in request state for link $linkName (Circuit ID = $circuit->{ID})");
        
    # Switch from the 'offline' to 'requesting' state, save to disk and store this in our memory
    eval {                                                                                                            
        $circuit->registerRequest($self->{BACKEND_TYPE}, $lifetime, $bandwidth);   
        $self->{CIRCUITS}{$linkName} = $circuit;
        $circuit->saveState();
        
        # Start the watchdog in case the request times out        
        $self->delayAdd($kernel, $ownHandles->{HANDLE_TIMER}, $circuit->{CIRCUIT_REQUEST_TIMEOUT}, CIRCUIT_TIMER_REQUEST, $circuit);
              
        $kernel->post($session, $backHandles->{BACKEND_REQUEST}, $circuit, $ownHandles->{REQUEST_REPLY});
    };

}

# This method is called when a circuit request fails.
# This is either because the request itself failed (got a reply and an error code) or
# the request timed out. In either case, this is obviously bad and what needs doing
# is the same in both cases
sub handleRequestFailure {
    my ($self, $circuit, $code) = @_;
    
    my $msg = "CircuitManager->handleRequestFailure";
    
    if (!defined $circuit) {
        $self->Logmsg("$msg: No circuit was provided");
        return;        
    }    
    
    my $linkName = $circuit->getLinkName();
    
    if ($circuit->{STATUS} != CIRCUIT_STATUS_REQUESTING) {
        $self->Logmsg("$msg: Can't do anything with this circuit");
        return;
    }
    
    # We got a response for the request - we need to remove the timer set in case the request timed out 
    $self->delayRemove($poe_kernel, CIRCUIT_TIMER_REQUEST, $circuit);
    
    eval {
        $self->Logmsg("$msg: Updating internal data");
        # Remove the state that was saved to disk
        $circuit->removeState();
        
        # Remove from hash of all circuits, then add it to the historical list
        delete $self->{CIRCUITS}{$linkName};
        $self->addCircuitToHistory($circuit);
        
        
        my $now = &mytimeofday();
        
        # Update circuit object internal data as well
        $circuit->registerRequestFailure($code);
        $circuit->saveState();
        
        # Blacklist this link 
        # This needs to be done *after* we register the failure with the circuit
        $self->addLinkToBlacklist($circuit, CIRCUIT_REQUEST_FAILED);
    }
        
}

sub handleRequestResponse {
    my ($self, $kernel, $session, $circuit, $return, $code) = @_[ OBJECT, KERNEL, SESSION, ARG0, ARG1, ARG2 ];

    my $msg = "CircuitManager->$ownHandles->{REQUEST_REPLY}"; 
            
    if (!defined $circuit || !defined $code) {
        $self->Logmsg("$msg: Circuit or code not defined");
        return;        
    }  
    
    my $linkName = $circuit->getLinkName();
    
    if ($circuit->{STATUS} != CIRCUIT_STATUS_REQUESTING) {
        $self->Logmsg("$msg: Can't do anything with this circuit");
        return;
    }
    
    # If the circuit request failed, call the method handling request failures
    if ($code < 0) {
        $self->Logmsg("$msg: Circuit request failed for $linkName");
        $self->handleRequestFailure($circuit, $code);        
        return;        
    } 
    
    # We got a response for the request - we need to remove the timer set in case the request timed out 
    $self->delayRemove($kernel, CIRCUIT_TIMER_REQUEST, $circuit);
    
    # If the circuit request succeeded ... yay   
    $self->Logmsg("$msg: Circuit request succeeded for $linkName"); 
    $circuit->removeState(); 
    $circuit->registerEstablished($return->{FROM_IP}, $return->{TO_IP}, $return->{BANDWIDTH});    
    $circuit->saveState();            
      
    $self->{CIRCUITS}{$linkName} = $circuit;
    if (defined $circuit->{LIFETIME}) {
        $self->Logmsg("$msg: Circuit has an expiration date. Starting countdown to teardown");        
        $self->delayAdd($kernel, $ownHandles->{HANDLE_TIMER}, $circuit->{LIFETIME}, CIRCUIT_TIMER_TEARDOWN, $circuit);
    }
}

sub handleTimer {
    my ($self, $kernel, $session, $timerType, $circuit) = @_[ OBJECT, KERNEL, SESSION, ARG0, ARG1];
    
    my $msg = "CircuitManager->$ownHandles->{HANDLE_TIMER}";
    
    if (!defined $timerType || !defined $circuit) {
        $self->Logmsg("$msg: Don't know how to handle this timer");
        return;        
    }
    
    my $linkName = $circuit->getLinkName();
    
    switch ($timerType) {
        case CIRCUIT_TIMER_REQUEST {
            $self->Logmsg("$msg: Timer for circuit request on link ($linkName) has expired");
            $self->handleRequestFailure($circuit, CIRCUIT_REQUEST_FAILED_TIMEDOUT);
        }
        case CIRCUIT_TIMER_BLACKLIST {
            $self->Logmsg("$msg: Timer for blacklisted link ($linkName) has expired");
            $self->handleTrimBlacklist($circuit);  
        }
        case CIRCUIT_TIMER_TEARDOWN {
            $self->Logmsg("$msg: Life for circuit ($circuit->{ID}) has expired");
            $self->handleCircuitTeardown($kernel, $session, $circuit);
        }
    }
}

# Circuits can be blacklisted for two reasons
# 1. A circuit request previously failed
#   - to prevent successive multiple retries to the same IDC, we temporarily blacklist that particular link
# 2. Multiple files in a job failed while being transferred on the circuit
#   - if transfers fail because of a circuit error, by default PhEDEx will retry transfers on the same link
#   we temporarily blacklist that particular link and PhEDEx will retry on a "standard" link instead
sub handleTrimBlacklist {
    my ($self, $circuit) = @_;
    return if ! defined $circuit;    
    my $linkName = $circuit->getLinkName();    
    $self->Logmsg("CircuitManager->handleTrimBlacklist: Removing $linkName from blacklist");
    delete $self->{LINKS_BLACKLISTED}{$linkName} if defined $self->{LINKS_BLACKLISTED}{$linkName};
    $self->delayRemove($poe_kernel, CIRCUIT_TIMER_BLACKLIST, $circuit);
}

sub handleCircuitTeardown {
    my ($self, $kernel, $session, $circuit) = @_;
    
    my $msg = "CircuitManager->handleCircuitTeardown";
    
    if (!defined $circuit) {
        $self->Logmsg("$msg: something went horribly wrong... Didn't receive a circuit back");
        return;        
    } 
    
    $self->delayRemove($kernel, CIRCUIT_TIMER_TEARDOWN, $circuit);
    
    my $linkName = $circuit->getLinkName();
    $self->Logmsg("$msg: Updating states for link $linkName");
    
    eval {
        $circuit->removeState();
            
        # Remove from hash of all circuits, then add it to the historical list
        delete $self->{CIRCUITS}{$linkName};
        $self->addCircuitToHistory($circuit);        
            
        # Update circuit object data  
        $circuit->registerTakeDown();
                    
        # Potentiall save the new state for debug purposes
        $circuit->saveState();
    };
    
    $self->Logmsg("$msg: Calling teardown for $linkName");
    
    # Call backend to take down this circuit
    $kernel->post($session, $backHandles->{BACKEND_TEARDOWN}, $circuit);    
}

# Cancels all requests in progress and tears down all the circuits that have been established
sub teardownAll {
    my ($self) = @_;
    
    my $msg = "CircuitManager->teardownAll";
    
    $self->Logmsg("$msg: Cleaning out all circuits");
    
    foreach my $circuit (values %{$self->{CIRCUITS}}) {
        my $backend = $self->{BACKEND};        
        switch ($circuit->{STATUS}) {
            case CIRCUIT_STATUS_REQUESTING {
                # TODO: Check and see if you can cancel requests
                # $backend->cancel_request($circuit);
            }
            case CIRCUIT_STATUS_ONLINE {
                $self->Logmsg("$msg: Tearing down circuit for link $circuit->getLinkName()");
                $self->handleCircuitTeardown($poe_kernel, $poe_kernel->ID_id_to_session($self->{SESSION_ID}), $circuit);
            }          
        }    
    }
}


# Adds a delay and keep the alarm ID which is returned in memory
# It has basically the same effect as delay_add, however, by having the
# alarm ID, we can cancel the timer if we know we don't need it anymore
# for ex. cancel the request time out when we get a reply, or 
# cancel the lifetime timer, if we need to destroy the circuit prematurely

# Depending on the architecture each tick of a delay adds about 10ms of overhead
sub delayAdd {
    my ($self, $kernel, $handle, $timer, $timerType, $circuit) = @_;
    
    # Set a delay for a given event, then get the ID of this timer    
    my $eventID = $kernel->delay_set($handle, $timer, $timerType, $circuit);
    
    # Remember this ID in order to clean it immediately after which we recevied an answer
    $self->{DELAYS}{$timerType}{$circuit->{ID}} = $eventID;
}

# Remove an alarm/delay before the trigger time
sub delayRemove {
    my ($self, $kernel, $timerType, $circuit) = @_;
    
    # Get the ID for the specified timer
    my $eventID = $self->{DELAYS}{$timerType}{$circuit->{ID}};
    
    # Remove from CircuitManager and remove from POE
    delete $self->{DELAYS}{$timerType}{$circuit->{ID}};
    $kernel->alarm_remove($eventID);    
}

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

1;