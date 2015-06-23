=head1 NAME

Helpers::Tasks::TaskManager - Task manager mainly used by the backends

=head1 DESCRIPTION

This is a helper class, allowing running and interaction with external processes.

It allows for multiple different tasks to be run at the same time.

=cut
package PHEDEX::File::Download::Circuits::Helpers::Tasks::TaskManager;

use Moose;

use base 'PHEDEX::Core::Logging';
use base 'Exporter';
our @EXPORT = qw(EXTERNAL_PID EXTERNAL_EVENTNAME EXTERNAL_OUTPUT EXTERNAL_TASK);

use POE;
use POE::Component::Child;

use PHEDEX::File::Download::Circuits::Helpers::Tasks::Task;

use constant {
    EXTERNAL_PID                    =>          0,
    EXTERNAL_EVENTNAME              =>          1,
    EXTERNAL_OUTPUT                 =>          2,
    EXTERNAL_TASK                   =>          3,
};

=head1 ATTRIBUTES

=item C<runningTasks>

=over

Moose hash of Helpers::Tasks::Task objects having the task PID as key.

The Moose system provides several helper methods: I<addTask>, I<getTask>, I<hasTask>, I<removeTask> and I<clearTasks>

=back

=cut 
has 'runningTasks'  => (is  => 'ro', isa => 'HashRef[PHEDEX::File::Download::Circuits::Helpers::Tasks::Task]',
                        traits  => ['Hash'], 
                        handles => {addTask     => 'set',
                                    getTask     => 'get',
                                    hasTask     => 'exists',
                                    removeTask  => 'delete', 
                                    clearTasks  => 'clear'});
has 'verbose'       => (is  => 'rw', isa => 'Bool', default => 0);

=head1 METHODS

=over
 
=item C<startCommand>

Launches an external process. If an action is specified (callback/postback), 
it will be called for each event (STDOUT, STDERR, SIGCHLD) with the following 
arguments: 
    
    PID, source event name, output

If a timeout is specified (in seconds), the task will be terminated 
(via SIGINT) if no output is received from STDOUT/STDERR withing the allotted time frame

=cut
sub startCommand {
    my ($self, $command, $action, $timeout) = @_;

    my $pid;

    my $msg = "TaskManager->startCommand";
    # TODO: Extra checks potentially needed on the type of command that needs to run
    if (!defined $command) {
        $self->Logmsg('$msg: Cannot start external tool without correct parameters');
        return 0;
    }

#    $self->Logmsg('$msg: No action has been specified for this task (really?)') if (! defined $action);

    # Create a separate session for each of the tools that we want to run
    # Alternatively, we could also use the POE::Component:Child wrapper, which does a similar thing
    POE::Session->create(
        inline_states => {
            _start =>  sub {
                my ($kernel, $heap) = @_[KERNEL, HEAP];

                # Start a new wheel running the specified command
                my $task = POE::Wheel::Run->new(
                    Program         => $command,
                    Conduit         => "pty-pipe",

                    StdoutEvent     => "handleTaskStdOut",
                    StderrEvent     => "handleTaskStdError",
                    ErrorEvent      => "handleTaskFailed",
                    CloseEvent      => "handleTaskClose",

                    StdioDriver     => POE::Driver::SysRW->new(),
                    StdinFilter     => POE::Filter::Line->new(Literal => "\n"),
                );

                $pid = $task->PID;

                # Set which event will handle the SIGCHLD signal
                $kernel->sig_child($pid, "handleTaskSignal");

                # Add the task to the heap (or else it will go out of scope)
                $heap->{tasks_by_id}{$task->ID} = $task;
                $heap->{tasks_by_pid}{$pid} = $task;

                # We also need to remember the currently running tasks and the actions that need to be taken for each
                
                my $taskWrapper = PHEDEX::File::Download::Circuits::Helpers::Tasks::Task->new(task => $task, action => $action);

                # If a timeout is defined, set an delay and remember the ALARM_ID
                if (defined $timeout) {
                    $self->Logmsg("$msg: Setting timeout for task");
                    $taskWrapper->alarmId($kernel->delay_set('handleTaskTimeout', $timeout, $heap, $pid));
                    $taskWrapper->alarmTimeout($timeout);
                }

                $self->addTask($pid, $taskWrapper);
            }
        },
        object_states => [
            $self => {
                handleTaskStdOut    =>  'handleTaskStdOut',
                handleTaskStdError  =>  'handleTaskStdOut',
                handleTaskClose     =>  'handleTaskClose',
                handleTaskSignal    =>  'handleTaskSignal',
                handleTaskTimeout   =>  'handleTaskTimeout',
            }
        ]
    );

    return $pid;
}

=item C<handleTaskStdOut>

Wheel event for both the StdOut and StdErr output. The action specified for this task will be called with
 with the following parameters:
    
    PID, event name, output
    
Output will be handled to the specified action and parsed there (not by this class)

=cut
sub handleTaskStdOut {
    my ($self, $sendingEvent, $heap, $output, $wheelId) = @_[OBJECT, STATE, HEAP, ARG0, ARG1];

    my $msg = "TaskManager->handleTaskStdOut";

    my $task = $heap->{tasks_by_id}{$wheelId};
    my $pid = $task->PID;
    my $action = $self->getTask($task->PID)->action;

    # Tick, so we know that the task is still alive
    $self->timerTick($pid);
    $self->Logmsg("$msg: $pid - $output")
    if $self->{VERBOSE};

    # If an action was specified, call it
    if (defined $action) {
        my @arguments;
        $arguments[EXTERNAL_TASK] = $task;
        $arguments[EXTERNAL_PID] = $pid;
        $arguments[EXTERNAL_EVENTNAME] = $sendingEvent;
        $arguments[EXTERNAL_OUTPUT] = $output;
        $action->(@arguments);
    }
}

=item C<handleTaskClose>

Wheel event when the task closes its output handle.

=cut
sub handleTaskClose {
    my ($self, $sendingEvent, $heap, $wheelId) = @_[OBJECT, STATE, HEAP, ARG0];

    my $msg = "TaskManager->handleTaskClose";

    $self->Logmsg("$msg: Task closed its last output handle");
}

=item C<handleTaskSignal>

Signal event when the child exists. If there's an action to be done, it will be called.
Cleanup is done when everything is finished

=cut
sub handleTaskSignal {
    my ($self, $sendingEvent, $heap, $pid) = @_[OBJECT, STATE, HEAP, ARG1];

    my $msg = "TaskManager->handleTaskSignal";

    my $task = $heap->{tasks_by_pid}{$pid};
    my $action = $self->getTask($task->PID)->action;

    $self->cleanupTask($heap, $task);

    $self->Logmsg("$msg: Task ($pid) has been terminated");
        # If an action was specified, call it
    if (defined $action) {
        my @arguments;
        $arguments[EXTERNAL_PID] = $pid;
        $arguments[EXTERNAL_EVENTNAME] = $sendingEvent;
        $action->(@arguments);
    }
}

=item C<handleTaskTimeout>

Event called in case the tool does not reply within a given time.
This only applies if a timeout has been specified when 'startCommand' was issued.

=cut
sub handleTaskTimeout {
    my ($self, $kernel, $session, $sendingEvent, $heap, $pid) = @_[OBJECT, KERNEL, SESSION, STATE, ARG0, ARG1];

    my @results = @_;
    my $msg = "TaskManager->handleTaskTimeout";
    $self->Logmsg("$msg: Didn't receive any output from task in a long time. Killing task");

    $self->kill_task($pid);
}

=item C<timerTick>

Re-adjusts the alarm since the task is still alive

=cut
sub timerTick {
    my ($self, $pid) = @_;
    my $taskWrapper = $self->getTask($pid);
    if (defined $taskWrapper->alarmId) {
        POE::Kernel->alarm_adjust($taskWrapper->alarmId, $taskWrapper->alarmTimeout);
    }
}

=item C<cleanupTask>

- Cleans up the heap

- Removes defunct references from $self

- Removes the timeout timer that might have been set

=cut
sub cleanupTask {
    my ($self, $heap, $task) = @_;

    my $msg = "TaskManager->cleanupTask";

    if (!defined $heap || !defined $task) {
        $self->Logmsg("$msg: Cannot clean up with invalid parameters");
        return;
    }

    my $pid = $task->PID;

    $self->Logmsg("$msg: Cleaning up task ($pid)");
    delete $heap->{tasks_by_id}{$task->ID};
    delete $heap->{tasks_by_pid}{$pid};

    my $taskWrapper = $self->getTask($pid);
    if (defined $taskWrapper->alarmId) {
        $self->Logmsg("$msg: Removing timer for PID ($pid)");
        POE::Kernel->alarm_remove($taskWrapper->alarmId) ;
    }

    $self->removeTask($pid);
}

=item C<kill_task>

- Sends a SIGINT signal to the task

- TODO: Use a new timer in case this won't respond to it

=back

=cut
sub kill_task {
    my ($self, $pid) = @_;

    my $msg = "TaskManager->kill_task";

    if (! defined $self->hasTask($pid)) {
        $self->Logmsg("$msg: Cannot find any process with the specified PID $pid");
        return;
    }

    $self->Logmsg("$msg: Killing PID $pid (SIGINT)");
    $self->getTask($pid)->task->kill("INT");
}

1;