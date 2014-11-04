package PHEDEX::File::Download::Circuits::ManagedResource::Bandwidth;

use strict;
use warnings;

use base 'PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource', 'PHEDEX::Core::Logging', 'Exporter';
use Data::UUID;
use POE;
use Switch;

use PHEDEX::File::Download::Circuits::Constants;

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;

    my %params = (
            # Object params
            BANDWIDTH_STEP          =>  1,      # Given in Gbps
            BANDWIDTH_MIN           =>  0,      # Given in multiples of BANDWIDTH_STEP (0 accepted - actually means taking the link down) 
            BANDWIDTH_MAX           =>  1000,   # Given in multiples of BANDWIDTH_STEP
    );

    my %args = (@_);

    #   use 'defined' instead of testing on value to allow for arguments which are set to zero.
    map { $args{$_} = defined($args{$_}) ? $args{$_} : $params{$_} } keys %params;
    my $self = $class->SUPER::new(%args);

    bless $self, $class;

    return $self;
}

sub initResource {
    my ($self, $backend, $nodeA, $nodeB, $bidirectional) = @_;
    
    # Do our own initialisation
    $self->{STATE_DIR}.="/bod";
    $self->{STATUS} = STATUS_BOD_OFFLINE;
    
    return $self->SUPER::initResource($backend, BOD, $nodeA, $nodeB, $bidirectional);
}

sub getSaveName {
    my $self = shift;
    my ($filePath, $savePath, $saveTime);
    
    if ($self->{STATUS} == STATUS_BOD_OFFLINE) {
        $savePath = $self->{STATE_DIR}.'/offline';
    } else {
        $savePath = $self->{STATE_DIR}.'/online';
    }
    
    $saveTime = $self->{LAST_STATUS_CHANGE};
        
    if (!defined $savePath || !defined $saveTime || $saveTime <= 0) {
        $self->Logmsg("Bandwidth->getSaveName: Invalid parameters in generating a file name");
        return undef;
    }

    my $partialID = substr($self->{ID}, 1, 8);
    $filePath = $savePath.'/'.$self->{NAME}."-$partialID-".$self->SUPER::formattedTime($saveTime);
    
    return ($savePath, $filePath);
}

sub registerRequest {
    my ($self, $bandwidth) = @_;

    my $msg = 'Bandwidth->registerRequest';

    # Check that the input parameters are ok
    if (! defined $bandwidth || ($bandwidth % $self->{BANDWIDTH_STEP} != 0)) {
        $self->Logmsg("$msg: Invalid bandwidth supplied (must be a multiple of BANDWIDTH_STEP)");
        return ERROR_GENERIC;
    }
    
    # Check if what we're asking is not already here
    if ($self->{BANDWIDTH_ALLOCATED} > $bandwidth) {
        $self->Logmsg("$msg: Bandwidth ");
        return ERROR_GENERIC;
    }
    
    $self->{STATUS} = STATUS_BOD_UPDATING;
    $self->{LAST_STATUS_CHANGE} = &mytimeofday();

    $self->Logmsg("$msg: state has been switched to STATUS_BOD_UPDATING");

    return OK;
}

1;
