=head1 NAME

Backend::Core::ResourceRequest - Object used to pass parameters when requesting a resource to the backend

=head1 DESCRIPTION

When constructing it, it requires at least the site names between which the 
circuit will be created, and the callback handle which will be invoked
once there is a status update for the request (either fail or success)

=cut
package PHEDEX::File::Download::Circuits::Backend::Core::ResourceRequest;

use Moose;

use base 'PHEDEX::Core::Logging';

use PHEDEX::File::Download::Circuits::Common::Constants;

has 'siteA'         => (is  => 'rw', isa => 'Str', required => 1);
has 'siteB'         => (is  => 'rw', isa => 'Str', required => 1);
has 'bidirectional' => (is  => 'rw', isa => 'Bool', default => 1);
has 'lifetime'      => (is  => 'rw', isa => 'Num', default => 6*HOUR);
has 'callback'      => (is  => 'rw', isa => 'Ref', required => 1);
has 'bandwidth'     => (is  => 'rw', isa => 'Num');

1;