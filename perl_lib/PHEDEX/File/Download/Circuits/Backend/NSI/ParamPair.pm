=head1 NAME

Backend::NSI::ParamPair - Helper object

=head1 DESCRIPTION

Stores the argument and the default value for an NSI reservation parameter 

=cut

package PHEDEX::File::Download::Circuits::Backend::NSI::ParamPair;

use Moose;

has 'arg'     => (is  => 'rw', isa => 'Str', required => 1);
has 'value'   => (is  => 'rw', isa => 'Str', required => 1);

1;