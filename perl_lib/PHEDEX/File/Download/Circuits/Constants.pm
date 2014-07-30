package PHEDEX::File::Download::Circuits::Constants;

use warnings;
use strict;

use base 'Exporter';

our @EXPORT = qw(CIRCUIT_STATUS_REQUESTING CIRCUIT_STATUS_ONLINE CIRCUIT_STATUS_TEARING_DOWN CIRCUIT_STATUS_OFFLINE 
                 CIRCUIT_ERROR_SAVING CIRCUIT_ERROR_OPENING CIRCUIT_GENERIC_ERROR
                 CIRCUIT_OK CIRCUIT_FAILED_REQUEST CIRCUIT_FAILED_TRANSFERS
                 
                 CIRCUIT_REQUEST_SUCCEEDED 
                 CIRCUIT_REQUEST_FAILED CIRCUIT_REQUEST_FAILED_PARAMS CIRCUIT_REQUEST_FAILED_SLOTS CIRCUIT_REQUEST_FAILED_BW CIRCUIT_REQUEST_FAILED_IDC CIRCUIT_REQUEST_FAILED_TIMEDOUT
                 
                 CIRCUIT_TRANSFERS_FAILED CIRCUIT_ALREADY_REQUESTED CIRCUIT_BLACKLISTED CIRCUIT_UNAVAILABLE CIRCUIT_AVAILABLE CIRCUIT_INVALID
                 
                 CIRCUIT_TIMER_REQUEST CIRCUIT_TIMER_BLACKLIST CIRCUIT_TIMER_TEARDOWN
                 
                 MINUTE HOUR DAY
                 );

use constant {
    
    # Related to Circuit.pm         
    CIRCUIT_OK                          =>      1,      # Just tells us that the subroutine returned with success (aka without errors)
         
    CIRCUIT_STATUS_OFFLINE              =>      10,
    CIRCUIT_STATUS_REQUESTING           =>      12,
    CIRCUIT_STATUS_ONLINE               =>      11,        
    CIRCUIT_STATUS_TEARING_DOWN         =>      13,       
          
    CIRCUIT_ERROR_SAVING                =>      -20,
    CIRCUIT_ERROR_OPENING               =>      -21,
    CIRCUIT_GENERIC_ERROR               =>      -22,
    
    CIRCUIT_FAILED_REQUEST              =>      20,
    CIRCUIT_FAILED_TRANSFERS            =>      21,
    
    # Related to Core.pm and related backends
    CIRCUIT_REQUEST_SUCCEEDED       =>          1,
    CIRCUIT_REQUEST_FAILED          =>          -30,     # Generic failure message
    CIRCUIT_REQUEST_FAILED_PARAMS   =>          -31,     # Parameters supplied are not valid
    CIRCUIT_REQUEST_FAILED_SLOTS    =>          -32,     # No more circuit slots available
    CIRCUIT_REQUEST_FAILED_BW       =>          -33,     # Cannot supply bandwidth required
    CIRCUIT_REQUEST_FAILED_IDC      =>          -34,     # Cannot contact IDC
    CIRCUIT_REQUEST_FAILED_TIMEDOUT =>          -35,
    
    # Related to CircuitManager.pm
    CIRCUIT_AVAILABLE               =>          40,     # Go ahead and request a circuit 
    CIRCUIT_TRANSFERS_FAILED        =>          -40,     # This circuit has been blacklisted because too many transfers failed on it
    CIRCUIT_ALREADY_REQUESTED       =>          -41,    # Circuit had already been requested or is currently established
    CIRCUIT_BLACKLISTED             =>          -42,    # Circuits have been temporarily blacklisted on current link
    CIRCUIT_UNAVAILABLE             =>          -43,    # Circuits not supported on current link    
    CIRCUIT_INVALID                 =>          -44,    # Provided link is not a valid one
    
    CIRCUIT_TIMER_REQUEST           =>          50,     # Used to handle a timer which expired for a request
    CIRCUIT_TIMER_BLACKLIST         =>          51,     # Used to handle a timer which expired for a circuit which was blacklisted 
    CIRCUIT_TIMER_TEARDOWN          =>          52,     # Used to handle a timer which expired for a circuit's life
    
    # Time constants
    MINUTE  =>      60,
    HOUR    =>      3600,
    DAY     =>      24*3600,
};
                 
1;
