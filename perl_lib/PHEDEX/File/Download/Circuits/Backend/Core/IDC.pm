package PHEDEX::File::Download::Circuits::Backend::Core::IDC;

use strict;
use warnings;

sub new
{
    my $proto = shift;
    my $class = ref($proto) || $proto;

    my %params = (
            SITE_NAME       =>      undef,  # Name of the site from applicantion's point of view (ex. for PhEDEx: T2_ANSE_Geneva)
            ENDPOINT_NAME   =>      undef,  # Name of circuit endpoint servicing the site (ex. for NSI: name of the STP)

            
            IP          =>      undef,
            PORT        =>      undef,
            BANDWIDTH   =>      1000,
            MAX_LIFE    =>      6*3600,
    );

    my %args = (@_);

    #   use 'defined' instead of testing on value to allow for arguments which are set to zero.
    map { $args{$_} = defined($args{$_}) ? $args{$_} : $params{$_} } keys %params;
    my $self = \%args;

    bless $self, $class;

    return $self;
}

1;