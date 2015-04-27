package PHEDEX::File::Download::Circuits::Common::Constants;

use warnings;
use strict;

use base 'Exporter';

our @EXPORT = qw(
                MINUTE HOUR DAY
                OK ERROR_GENERIC ERROR_SAVING ERROR_OPENING ERROR_PARAMETER_UNDEF ERROR_FILE_NOT_FOUND ERROR_PATH_INVALID ERROR_INVALID_OBJECT
                );

use constant {
    # Time constants
    MINUTE  =>      60,
    HOUR    =>      3600,
    DAY     =>      24*3600,
    
    # Return codes
    OK                          => 0,
    ERROR_GENERIC               => -1,
    ERROR_SAVING                => -2,
    ERROR_OPENING               => -3,
    ERROR_PARAMETER_UNDEF       => -4,
    ERROR_FILE_NOT_FOUND        => -5,
    ERROR_PATH_INVALID          => -6,
    ERROR_INVALID_OBJECT        => -7,

};

1;