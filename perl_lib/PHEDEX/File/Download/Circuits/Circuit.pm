package PHEDEX::File::Download::Circuits::Circuit;

use strict;
use warnings;

use base 'PHEDEX::Core::Logging', 'Exporter';
use PHEDEX::Core::Timing;
use PHEDEX::Core::Command;
use PHEDEX::File::Download::Circuits::Constants;
use PHEDEX::File::Download::Circuits::TFCUtils;
use Data::Dumper;
use Data::UUID;
use POSIX;
use File::Path;
use Switch;
use Scalar::Util qw(blessed);

our @EXPORT = qw(compareCircuits openCircuit formattedTime getLink);

$Data::Dumper::Terse = 1;
$Data::Dumper::Indent = 1;

# Use registerRequest, registerEstablished, registerTakeDown and registerRequestFailure
# in order to ensure a consistent state change throughout the object's lifetime
# Do not modify these parameters directly! (unless you know what you're doing - then it's ok)

# Ideas for later on - unused parameters for now:
# - SCOPE: This can be used if we'd have multiple circuits per link
#       and we'd want a way to discriminate againts using some circuits over others 
#       (like possibly using different circuits for different protocols, etc.)
# - BANDWIDTH_REQUESTED and BANDWIDTH_USED: could be used to track the performance of the circuit. 
sub new
{
    my $proto = shift;
    my $class = ref($proto) || $proto;
    
    my %params = (
            # Object params
            ID                      =>  undef,
            BOOKING_BACKEND         =>  'Dummy',
            STATUS                  =>  CIRCUIT_STATUS_OFFLINE,
            SCOPE                   =>  'GENERIC',
            LAST_STATUS_CHANGE      =>  undef,
            PHEDEX_FROM_NODE        =>  undef,     
            PHEDEX_TO_NODE          =>  undef,
            LIFETIME                =>  undef,
            REQUEST_TIME            =>  undef,
            ESTABLISHED_TIME        =>  undef,
            CIRCUIT_TO_IP           =>  undef,
            CIRCUIT_FROM_IP         =>  undef,
            
            FAILURES                =>  {
                                            CIRCUIT_FAILED_REQUEST          =>      undef,
                                            CIRCUIT_FAILED_TRANSFERS        =>      [],
                                        },
            
            # Other parameters
            CIRCUITDIR            =>  undef,
            
            # General parameters
            CIRCUIT_REQUEST_TIMEOUT       => 5*MINUTE,         # in seconds
            CIRCUIT_DEFAULT_LIFETIME      => 5*HOUR,      # in seconds
            
            # Performance related parameters
            BANDWIDTH_REQUESTED     =>  undef,              # Bandwidth we requested
            BANDWIDTH_ALLOCATED     =>  undef,              # Bandwidth allocated
            BANDWIDTH_USED          =>  undef,              # Bandwidth we're actually using      
            
            VERBOSE                 =>  undef,      
    );
				
    my %args = (@_);
    
    #   use 'defined' instead of testing on value to allow for arguments which are set to zero.
    map { $args{$_} = defined($args{$_}) ? $args{$_} : $params{$_} } keys %params;
    my $self = $class->SUPER::new(%args);                   

    my $ug = new Data::UUID;     
    $self->{ID} = $ug->to_string($ug->create());
    $self->{CIRCUITDIR} = '/tmp/circuit' unless defined $self->{CIRCUITDIR};            
     
    bless $self, $class;
    
    return $self;    
}

# This method is normally used to compare circuits, but it 
# can be used to compare entire data structures as well...
# It will compare any object made of SCALAR, ARRAY and HASH
sub compareCircuits {
    my ($object1, $object2) = @_;

    # Not equal if one's defined and the other isn't        
    return 0 if (!defined $object1 == defined $object2);
    # Equal if both aren't defined
    return 1 if (!defined $object1 && !defined $object2);
      
    my ($dref1, $dref2) = (ref($object1), ref($object2));
    # Not equal if referenced types don't match
    return 0 if $dref1 ne $dref2;

    # Return simple comparison for variables passed by values       
    return $object1 eq $object2 if ($dref1 eq '');
            
    if ($dref1 eq 'SCALAR' || $dref1 eq 'REF') {
        return compareCircuits(${$object1}, ${$object2});
    } elsif ($dref1 eq 'ARRAY'){               
        # Not equal if array size differs
        return 0 if ($#{$object1} != $#{$object1});
        # Go through all the items - order counts!         
        for my $i (0 .. @{$object1}) {
            return 0 if ! compareCircuits($object1->[$i], $object2->[$i]);
        }
    } elsif ($dref1 eq 'HASH' || defined blessed($object1)) {
        # Not equal if they don't have the same number of keys
        return 0 if (scalar keys (%{$object1}) != scalar keys (%{$object2}));
        # Go through all the items
        foreach my $key (keys %{$object1}) {
            return 0 if ! compareCircuits($object1->{$key}, $object2->{$key});
        }    
    }

    # Equal, if we get to here
    return 1;    
}

sub setNodes {
    my ($self, $fromNode, $toNode) = @_;
    $self->Logmsg("Circuit->setNodes: Nodes set to $fromNode and $toNode") if ($self->{VERBOSE});
    $self->{PHEDEX_FROM_NODE} = $fromNode;
    $self->{PHEDEX_TO_NODE} = $toNode;   
}

# Returns the link name in the form of Node1-to-Node2 from two given nodes
sub getLink {
    my ($from, $to) = @_;
    return defined $from && defined $to ? $from."-to-".$to : undef;
}

# Calls getLink on its own parameters PHEDEX_FROM_NODE and PHEDEX_TO_NODE
sub getLinkName {
    my $self = shift;
    return getLink($self->{PHEDEX_FROM_NODE}, $self->{PHEDEX_TO_NODE});
}

# Returns the expiration time if LIFETIME was defined; undef otherwise
sub getExpirationTime {
    my $self = shift;
    return $self->{STATUS} == CIRCUIT_STATUS_ONLINE && 
           defined $self->{LIFETIME} ? $self->{ESTABLISHED_TIME} + $self->{LIFETIME} : undef;  
}

# Checks to see if the circuit expired or not (if LIFETIME was defined)
sub isExpired {
    my $self = shift;
    my $expiration = $self->getExpirationTime();
    return defined $expiration && $expiration < &mytimeofday() ? 1 : 0;  
}

# Method used to switch state from OFFLINE to REQUESTING
# Backend has to be provided when requesting circuits
sub registerRequest { 
    my ($self, $backend, $lifetime, $bandwidth) = @_;
    
    my $mess = 'Circuit->registerRequest';
    
    # Cannot change status to CIRCUIT_STATUS_REQUESTING if
    #   - The status is not prior CIRCUIT_STATUS_OFFLINE
    #   - PHEDEX_FROM_NODE and PHEDEX_TO_NODE are not defined
    if ($self->{STATUS} != CIRCUIT_STATUS_OFFLINE ||
        !defined $self->{PHEDEX_FROM_NODE} || !defined $self->{PHEDEX_TO_NODE} ||
        ! defined $backend) {
        $self->Logmsg("$mess: Cannot change status to CIRCUIT_STATUS_REQUESTING");
        return CIRCUIT_GENERIC_ERROR;
    }
    
    $self->{STATUS} = CIRCUIT_STATUS_REQUESTING;
    $self->{REQUEST_TIME} = &mytimeofday();
    $self->{LAST_STATUS_CHANGE} = $self->{REQUEST_TIME};
    $self->{BOOKING_BACKEND} = $backend;
    
    # These two parameters can be undef
    $self->{LIFETIME} = $lifetime;
    $self->{BANDWIDTH_REQUESTED} = $bandwidth;
    
    $self->Logmsg("$mess: state has been switched to CIRCUIT_STATUS_REQUESTING");
    
    return CIRCUIT_OK;
}

# Method used to switch state from REQUESTING to ONLINE
sub registerEstablished {
    my ($self, $circuit_from_ip, $circuit_to_ip, $bandwidth) = @_;
    
    my $mess = 'Circuit->registerEstablished';
    
    # Cannot change status to CIRCUIT_STATUS_ONLINE if
    #   - The status is not prior CIRCUIT_STATUS_REQUESTING 
    #   - both $circuit_from_ip and $circuit_to_ip are not valid addresses
    if ($self->{STATUS} != CIRCUIT_STATUS_REQUESTING ||        
        determineAddressType($circuit_from_ip) == ADDRESS_INVALID || 
        determineAddressType($circuit_to_ip) == ADDRESS_INVALID) {
        $self->Logmsg("$mess: Cannot change status to CIRCUIT_STATUS_ONLINE");
        return CIRCUIT_GENERIC_ERROR;
    }
    
    $self->{STATUS} = CIRCUIT_STATUS_ONLINE;
    $self->{ESTABLISHED_TIME} = &mytimeofday();
    $self->{LAST_STATUS_CHANGE} = $self->{ESTABLISHED_TIME};
    $self->{CIRCUIT_FROM_IP} = $circuit_from_ip;
    $self->{CIRCUIT_TO_IP} = $circuit_to_ip;
           
    # These two can also be undef
    $self->{BANDWIDTH_ALLOCATED} = $bandwidth;

    $self->Logmsg("$mess: state has been switched to CIRCUIT_STATUS_ONLINE");
    return CIRCUIT_OK;
}

# Method used to switch state from ONLINE to OFFLINE
sub registerTakeDown {
    my $self = shift;
    
    my $mess = 'Circuit->registerTakeDown';
    
    if ($self->{STATUS} != CIRCUIT_STATUS_ONLINE) {
        $self->Logmsg("$mess: Cannot change status to CIRCUIT_STATUS_OFFLINE");
        return CIRCUIT_GENERIC_ERROR;         
    }
    
    $self->{STATUS} = CIRCUIT_STATUS_OFFLINE;
    $self->{LAST_STATUS_CHANGE} = &mytimeofday();    
    
    $self->Logmsg("$mess: state has been switched to CIRCUIT_STATUS_OFFLINE");
    return CIRCUIT_OK;
}

# All failures should be tracked in {FAILURES} by (timestamp and reason)

# Method used to switch state from REQUESTING to OFFLINE
# It is recommended to provide a reason why this request failed
sub registerRequestFailure { 
    my ($self, $reason) = @_;
    
    my $mess = 'Circuit->registerRequestFailure';
    
    if ($self->{STATUS} != CIRCUIT_STATUS_REQUESTING) {
        $self->Logmsg("$mess: Cannot register a request failure for a circuit not CIRCUIT_STATUS_REQUESTING");
        return CIRCUIT_GENERIC_ERROR;         
    }
    
    $self->{STATUS} = CIRCUIT_STATUS_OFFLINE;
    $self->{LAST_STATUS_CHANGE} = &mytimeofday();
   
    # Keep track of why the request failed   
    my $failure = [$self->{LAST_STATUS_CHANGE}, $reason];
    $self->{FAILURES}{CIRCUIT_FAILED_REQUEST} = $failure;   
    
    $self->Logmsg("$mess: Circuit request failure has been registered");
    
    return CIRCUIT_OK;
}

# Returns an array with the [time, reason] of the failed request
sub getFailedRequest {
    my $self = shift;
    return $self->{FAILURES}{CIRCUIT_FAILED_REQUEST};
}


# Method used to keep track of how many transfers failed
# Based on this information CircuitManager might decide to blacklist a circuit
# if too many transfers failed on this particular circuit
sub registerTransferFailure {
    my ($self, $task) = @_;
    
    # TODO: When registering a failure, it might be nice to also clean up old ones or just "remember the last xxx failures"
    my $mess = 'Circuit->registerTransferFailure';
    
    if ($self->{STATUS} != CIRCUIT_STATUS_ONLINE) {
        $self->Logmsg("$mess: Cannot register a trasfer failure for a circuit not CIRCUIT_STATUS_ONLINE");
        return CIRCUIT_GENERIC_ERROR;         
    }
    
    my $failure = [&mytimeofday(), $task];   
    push(@{$self->{FAILURES}{CIRCUIT_FAILED_TRANSFERS}}, $failure);
    
    $self->Logmsg("$mess: Circuit transfer failure has been registered") if ($self->{VERBOSE});    
    return CIRCUIT_OK;
}

# Returns an array with all the details regarding the failed transfers
# that occured on this circuit. Each element in the array is in the form
# of [time, reason]
sub getFailedTransfers() {
    my $self = shift;
    return $self->{FAILURES}{CIRCUIT_FAILED_TRANSFERS};
}


# In case we want to re-use circuit objects, it resets all parameters to default
sub reset {
    my ($self) = @_;     
    
    # Reset all parameters
    foreach my $key (keys %{$self}) {
        next if ($key eq 'ID');
        $self->{$key} = undef;
    }
    
    $self->{STATUS} = CIRCUIT_STATUS_OFFLINE;
    $self->{LAST_STATUS_CHANGE} = &mytimeofday();        
}

# Saves the current state of the circuit 
# For this a valid CIRCUITDIR must be defined
# If it's not specified at construction it will automatically be created in {DROPDIR}/circuit
# If DROPDIR is not specified this defaults to '/tmp'
# Based on its current state, the circuit will either be saved in
# {CIRCUITDIR}/requested, {CIRCUITDIR}/online or {CIRCUITDIR}/offline
sub saveState {
    my $self = shift;
    
    my $mess = 'Circuit->saveState';
    
    # Don't save if it's not in either of these states: OFFLINE, REQUESTING, ONLINE
    if ($self->{STATUS} == CIRCUIT_STATUS_TEARING_DOWN) {
        $self->Logmsg("$mess: Won't save circuit - it is not \'in request\', \'online\' or \'offline\'...");
        return CIRCUIT_ERROR_SAVING;
    }
           
    # Generate file name based on 
    my ($savePath, $filePath) = _getSaveName($self);    
    if (! defined $filePath) {
        $self->Logmsg("$mess: An error has occured while generating file name");
        return CIRCUIT_ERROR_SAVING;
    }
    
    # Check if circuit state folder existed and create if it didn't
    if (!-d $savePath) {        
        File::Path::make_path($savePath, {error => \my $err});
        if (@$err) {
            $self->Logmsg("$mess: Circuit state folder did not exist and we were unable to create it");
            return CIRCUIT_ERROR_SAVING;
        }
    }
    
    # Save the circuit
    my $file = &output($filePath, Dumper($self));    
    if (! $file) {
        $self->Logmsg("$mess: Unable to save circuit state information");
        return CIRCUIT_ERROR_SAVING;
    } else {
        $self->Logmsg("$mess: Circuit state information successfully saved");
        return CIRCUIT_OK;    
    };
  
}

# Attempts to remove the state file associated with this circuit
sub removeState {
    my $self = shift;
    
    my $mess = 'Circuit->removeState';
    
    my ($savePath, $filePath) = _getSaveName($self);
    if (!-d $savePath || !-e $filePath) {
        $self->Logmsg("$mess: There's nothing to remove from the state folders");
        return CIRCUIT_GENERIC_ERROR;
    }
    
    return !(unlink $filePath) ? CIRCUIT_GENERIC_ERROR : CIRCUIT_OK;
}

# Factory like method : returns a new circuit from a state file on disk
# It will throw an error if the circuit provided is corrupt
# i.e. it doesn't have an ID, STATUS or PHEDEX TO/FROM nodes defined
sub openCircuit {
    my ($path) = @_;
    
    return (undef, CIRCUIT_ERROR_OPENING) unless (-e $path);

    my $circuit = &evalinfo($path);
    
    if (! defined $circuit->{ID} || 
        ! defined $circuit->{STATUS} ||
        ! defined $circuit->{PHEDEX_FROM_NODE} || ! defined $circuit->{PHEDEX_TO_NODE}) {
        return (undef, CIRCUIT_ERROR_OPENING);        
    }
            
    return ($circuit, CIRCUIT_OK);
}

# Generates a file name in the form of : FROM_NODE-to-TO_NODE-time
# ex. T2_ANSE_Amsterdam-to-T2_ANSE_Geneva-20140427-10:00:00
# Returns a save path ({CIRCUITDIR}/$state) and a file path ({CIRCUITDIR}/$state/$FROM_NODE-to-$TO_NODE-$time)
# We could also put part of the UUID at the end of the file but for now it is not needed
# unless we would request multiple circuits on the same link *at the same time*...which we don't/won't?
sub _getSaveName() {
    my $self = shift;
    
    my ($filePath, $savePath, $saveTime);
    
    switch ($self->{STATUS}) {
        case CIRCUIT_STATUS_REQUESTING {
            $savePath = $self->{CIRCUITDIR}.'/requested';
            $saveTime = $self->{REQUEST_TIME};
        }
        case CIRCUIT_STATUS_ONLINE {
            $savePath = $self->{CIRCUITDIR}.'/online';
            $saveTime = $self->{ESTABLISHED_TIME};     
        }
        case CIRCUIT_STATUS_OFFLINE {
            $savePath = $self->{CIRCUITDIR}.'/offline';
            $saveTime = $self->{LAST_STATUS_CHANGE};     
        }
          
    }

    if (!defined $savePath || !defined $saveTime || $saveTime <= 0) {
        $self->Logmsg("Circuit->_getSaveName: Invalid parameters in generating a circuit file name");
        return undef;
    }    

    $filePath = $savePath.'/'.$self->getLinkName().'-'.formattedTime($saveTime);
    
    return ($savePath, $filePath);    
}

# Generates a human readable date and time - mostly used when saving, in the state file name
sub formattedTime{ 
    my $time = shift;    
    return defined time ? strftime ('%Y%m%d-%Hh%Mm%S', gmtime(int($time))) : undef; 
}

1;