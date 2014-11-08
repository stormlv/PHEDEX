package PHEDEX::File::Download::Circuits::Constants;

use warnings;
use strict;

use base 'Exporter';

our @EXPORT = qw(
                BOD CIRCUIT
                OK
                ERROR_GENERIC ERROR_SAVING ERROR_OPENING
                STATUS_CIRCUIT_OFFLINE STATUS_CIRCUIT_REQUESTING STATUS_CIRCUIT_ONLINE
                STATUS_BOD_OFFLINE STATUS_BOD_UPDATING STATUS_BOD_ONLINE
                CIRCUIT_REQUEST_SUCCEEDED
                CIRCUIT_REQUEST_FAILED CIRCUIT_REQUEST_FAILED_PARAMS CIRCUIT_REQUEST_FAILED_SLOTS CIRCUIT_REQUEST_FAILED_BW CIRCUIT_REQUEST_FAILED_IDC CIRCUIT_REQUEST_FAILED_TIMEDOUT
                EXTERNAL_PID EXTERNAL_EVENTNAME EXTERNAL_OUTPUT 
                CIRCUIT_TRANSFERS_FAILED RESOURCE_ALREADY_EXISTS LINK_BLACKLISTED RESOURCE_TYPE_UNSUPPORTED LINK_UNSUPPORTED RESOURCE_REQUEST_POSSIBLE CIRCUIT_INVALID
                CIRCUIT_TIMER_REQUEST CIRCUIT_TIMER_BLACKLIST CIRCUIT_TIMER_TEARDOWN
                BOD_UPDATE_REDUNDANT
                MINUTE HOUR DAY
                );

use constant {
    BOD                 =>  "Bandwidth on demand",
    CIRCUIT             =>  "Circuit",
    
    OK                          => 0,

    # Error constants
    ERROR_GENERIC               => -1,
    ERROR_SAVING                => -2,
    ERROR_OPENING               => -3,
    
    # Status constants
    STATUS_CIRCUIT_OFFLINE      => 1,
    STATUS_CIRCUIT_REQUESTING   => 2,
    STATUS_CIRCUIT_ONLINE       => 3,
    
    STATUS_BOD_OFFLINE          => 4,
    STATUS_BOD_UPDATING         => 5,
    STATUS_BOD_ONLINE           => 6,
    
    CIRCUIT_FAILED_REQUEST          =>      20,
    CIRCUIT_FAILED_TRANSFERS        =>      21,

    # Related to Core.pm and related backends
    CIRCUIT_REQUEST_SUCCEEDED       =>          1,
    CIRCUIT_REQUEST_FAILED          =>          -30,     # Generic failure message
    CIRCUIT_REQUEST_FAILED_PARAMS   =>          -31,     # Parameters supplied are not valid
    CIRCUIT_REQUEST_FAILED_SLOTS    =>          -32,     # No more circuit slots available
    CIRCUIT_REQUEST_FAILED_BW       =>          -33,     # Cannot supply bandwidth required
    CIRCUIT_REQUEST_FAILED_IDC      =>          -34,     # Cannot contact IDC
    CIRCUIT_REQUEST_FAILED_TIMEDOUT =>          -35,

    EXTERNAL_PID                    =>          0,     # PID Index in arguments passed back via an action by External.pm
    EXTERNAL_EVENTNAME              =>          1,
    EXTERNAL_OUTPUT                 =>          2,

    # TODO: Revisit this and rename with BOD in mind
    # Related to CircuitManager.pm
    
    CIRCUIT_TRANSFERS_FAILED        =>          -41,    # This circuit has been blacklisted because too many transfers failed on it
    CIRCUIT_INVALID                 =>          -42,    # Provided link is not a valid one

    RESOURCE_REQUEST_POSSIBLE       =>          40,     # Go ahead and request a circuit
    RESOURCE_ALREADY_EXISTS         =>          -43,    # A resource had already been previously requested for a given link
    RESOURCE_TYPE_UNSUPPORTED       =>          -44,    # Backend does not supported the management of requested resource type
    LINK_BLACKLISTED                =>          -45,    # Temporarily cannot use managed resources on current link
    LINK_UNSUPPORTED                =>          -46,    # Circuits not supported on current link
    
    CIRCUIT_TIMER_REQUEST           =>          50,     # Used to handle a timer which expired for a request
    CIRCUIT_TIMER_BLACKLIST         =>          51,     # Used to handle a timer which expired for a circuit which was blacklisted
    CIRCUIT_TIMER_TEARDOWN          =>          52,     # Used to handle a timer which expired for a circuit's life
    
    # 
    BOD_UPDATE_REDUNDANT            =>          60,     # An updated of BoD has been requested, but requested bandwidth is already commissioned
    
    # Time constants
    MINUTE  =>      60,
    HOUR    =>      3600,
    DAY     =>      24*3600,
};

1;

