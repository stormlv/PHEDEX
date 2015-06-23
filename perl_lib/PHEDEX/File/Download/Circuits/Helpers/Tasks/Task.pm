=head1 NAME

Helpers::Tasks::Task - Helper object

=head1 DESCRIPTION

Stores parameters which define a task. 

The task parameter is required and is the POE Wheel which is created when
we run a process. 

=cut

package PHEDEX::File::Download::Circuits::Helpers::Tasks::Task;

use Moose;

has 'task'          => (is  => 'ro', isa => 'Ref', required => 1);
has 'action'        => (is  => 'ro', isa => 'Ref');
has 'alarmId'       => (is  => 'rw', isa => 'Int');
has 'alarmTimeout'  => (is  => 'rw', isa => 'Int', default => 10);
1;
