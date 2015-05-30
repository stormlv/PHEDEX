package PHEDEX::File::Download::Circuits::Backend::NSI::ReservationParam;

use Moose;

use PHEDEX::File::Download::Circuits::Backend::NSI::ParamPair;

has 'bandwidth'     => (is  => 'rw', isa        => 'PHEDEX::File::Download::Circuits::Backend::NSI::ParamPair',
                                     default    => sub { PHEDEX::File::Download::Circuits::Backend::NSI::ParamPair->new(arg => '--bw', value => 1000)});
has 'description'   => (is  => 'rw', isa        => 'PHEDEX::File::Download::Circuits::Backend::NSI::ParamPair',
                                     default    => sub { PHEDEX::File::Download::Circuits::Backend::NSI::ParamPair->new(arg => '--d', value => "Test circuit ".int(rand(100000)))});
has 'startTime'     => (is  => 'rw', isa        => 'PHEDEX::File::Download::Circuits::Backend::NSI::ParamPair',
                                     default    => sub { PHEDEX::File::Download::Circuits::Backend::NSI::ParamPair->new(arg => '--st', value => "10 sec")});
has 'endTime'       => (is  => 'rw', isa        => 'PHEDEX::File::Download::Circuits::Backend::NSI::ParamPair',
                                     default    => sub { PHEDEX::File::Download::Circuits::Backend::NSI::ParamPair->new(arg => '--et', value => "30 min")});
#has 'gri'           => (is  => 'rw', isa        => 'PHEDEX::File::Download::Circuits::Backend::NSI::ParamPair',
#                                     default    => sub { PHEDEX::File::Download::Circuits::Backend::NSI::ParamPair->new(arg => '--g', value => "PhEDEx-NSI")});
has 'sourceStp'     => (is  => 'rw', isa        => 'PHEDEX::File::Download::Circuits::Backend::NSI::ParamPair',
                                     default    => sub { PHEDEX::File::Download::Circuits::Backend::NSI::ParamPair->new(arg => '--ss', value => "urn:ogf:network:somenetwork:somestp?vlan=333")});
has 'destinationStp'=> (is  => 'rw', isa        => 'PHEDEX::File::Download::Circuits::Backend::NSI::ParamPair',
                                     default    => sub { PHEDEX::File::Download::Circuits::Backend::NSI::ParamPair->new(arg => '--ds', value => "urn:ogf:network:othernetwork:otherstp?vlan=333")});

1;