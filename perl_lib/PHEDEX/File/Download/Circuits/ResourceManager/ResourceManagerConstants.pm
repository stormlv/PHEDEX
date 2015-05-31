package PHEDEX::File::Download::Circuits::ResourceManager::ResourceManagerConstants;

use warnings;
use strict;

use base 'Exporter';

our @EXPORT = qw(
                EXTERNAL_PID EXTERNAL_EVENTNAME EXTERNAL_OUTPUT EXTERNAL_TASK
                BOD_UPDATE_REDUNDANT
                );

use constant {
    CIRCUIT_FAILED_REQUEST          =>      20,
    CIRCUIT_FAILED_TRANSFERS        =>      21,
 
    BOD_UPDATE_REDUNDANT            =>          60,     # An updated of BoD has been requested, but requested bandwidth is already commissioned
   
};

1;

