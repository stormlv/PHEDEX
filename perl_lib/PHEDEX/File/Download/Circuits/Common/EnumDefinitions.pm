package PHEDEX::File::Download::Circuits::Common::EnumDefinitions;

use warnings;
use strict;

use base 'Exporter';

our @EXPORT = qw(
                LAYER_TYPES RESOURCE_TYPES SCOPE_TYPES STATUS_TYPES NSI_STATES
                );

use constant {
    LAYER_TYPES     => [qw(Layer1 Layer2 Layer3)],
    RESOURCE_TYPES  => [qw(Circuit Bandwidth)],
    SCOPE_TYPES     => [qw(Generic Analysis Simulation)],
    STATUS_TYPES    => [qw(Offline Pending Online)],
    NSI_STATES      => [qw(Created AssignedId Confirmed Commited Provisioned Active Terminated Error ConfirmFail CommitFail ProvisionFail)]
};

1;