=head1 NAME

NSI::NSI - NSI backend

=head1 DESCRIPTION

This is the NSI backend, which extends the Core::Core class.
It uses the NSI CLI to issue commands to an NSI aggregator.
Currently, it can request and terminate circuits. The update reservation 
function hasn't been implemented yet since the update doesn't seem to be
working in the NSI CLI properly.s

=cut

package PHEDEX::File::Download::Circuits::Backend::NSI::NSI;

use Moose;

extends 'PHEDEX::File::Download::Circuits::Backend::Core::Core';

use base 'PHEDEX::Core::Logging';

# PhEDEx imports
use PHEDEX::File::Download::Circuits::Backend::Core::Core;
use PHEDEX::File::Download::Circuits::Backend::NSI::Action;
use PHEDEX::File::Download::Circuits::Backend::NSI::Reservation;
use PHEDEX::File::Download::Circuits::Backend::NSI::StateMachine;
use PHEDEX::File::Download::Circuits::Helpers::Tasks::TaskManager;

# Other imports
use Data::UUID;
use LWP::Simple;
use POE;
use Switch;

use constant {
    UPDATE      => "Update",
    REQUEST     => "Request",
    TEARDOWN    => "Teardown",
};

=head1 ATTRIBUTES

=over
 
=item C<taskManager>

In general, the task manager executes external commands.
Here, it calls the NSI CLI and forwards inputs that were passed to it by the backend.
All output for this thread is redirected to the "processToolOutput" method. 

=cut 
has 'taskManager'       => (is  => 'rw', isa => 'PHEDEX::File::Download::Circuits::Helpers::Tasks::TaskManager', default => sub { PHEDEX::File::Download::Circuits::Helpers::Tasks::TaskManager->new() });

=item C<actionHandler>

Is the postback to the processToolOutput method. It is called by the task manager 
on each output to the StdOut/Err of the NSI CLI

=cut 
has 'actionHandler'     => (is  => 'rw', isa => 'Ref');

=item C<timeout>

Timeout used for the task manager

=cut 
has 'timeout'           => (is  => 'rw', isa => 'Int', default => 120);

=item C<actionQueue>

Moose array of Backend::NSI::Action objects. It's used as a queue for the actions 
to be performed by the backend.

The Moose system provides several helper methods: I<queueAction>, I<dequeueAction> and I<actionQueueSize>

=cut 
has 'actionQueue'       => (is  => 'rw', isa => 'ArrayRef[PHEDEX::File::Download::Circuits::Backend::NSI::Action]', 
                            traits => ['Array'],
                            handles => {queueAction     => 'push', 
                                        dequeueAction   => 'shift', 
                                        actionQueueSize => 'count'});

=item C<currentAction>

Retains the current action that is being done.

When doing typing ('isa') in Moose, you're not allowed to set the value to undef by hand.
This is why a I<clearer> is provided I<clearCurrentAction> to set it to undef.

The predicate hasCurrentAction returns if currentAction is set.

=cut
has 'currentAction'     => (is  => 'rw', isa => 'PHEDEX::File::Download::Circuits::Backend::NSI::Action',
                            clearer     => 'clearCurrentAction',
                            predicate   => 'hasCurrentAction');
                            
=item C<reservations>

Moose hash of Backend::NSI::Reservation objects. It holds all the active reservations for this backend.

The Moose system provides several helper methods: I<addReservation>, I<getReservation>,  I<removeReservation> and I<hasReservation>

=cut 
has 'reservations'      => (is  => 'rw', isa => 'HashRef[PHEDEX::File::Download::Circuits::Backend::NSI::Reservation]', 
                            traits => ['Hash'],  
                            handles => {addReservation      => 'set',
                                        getReservation      => 'get',
                                        removeReservation   => 'delete',
                                        hasReservation      => 'exists'});
=item C<nsiToolLocation> C<nsiTool> C<nsiToolJavaFlags> C<nsiToolPid> C<command> 

The different parameters needed to run the NSI CLI. Command is a concatenation of the previous attributes.
PID is given after the taskmanager runs the command

=cut 
has 'nsiToolLocation'   => (is  => 'rw', isa => 'Str', default => '/data/NSI/CLI');
has 'nsiTool'           => (is  => 'rw', isa => 'Str', default => 'nsi-cli-1.2.1-one-jar.jar');
has 'nsiToolJavaFlags'  => (is  => 'rw', isa => 'Str', default =>   '-Xmx256m -Djava.net.preferIPv4Stack=true '.
                                                                    '-Dlog4j.configuration=file:./config/log4j.properties '.
                                                                    '-Dcom.sun.xml.bind.v2.runtime.JAXBContextImpl.fastBoot=true '.
                                                                    '-Dorg.apache.cxf.JDKBugHacks.defaultUsesCaches=true ');
has 'nsiToolPid'        => (is  => 'rw', isa => 'Int');
has 'command'           => (is  => 'rw', isa => 'Str');

=item C<defaultProvider>

This is the script used to configure the provider (aggregator)

It should also have the truststore containing the aggregator server certificats (store password is in: provider-client-https-cc.xml)

=cut 
has 'defaultProvider'   => (is  => 'rw', isa => 'Str', default => 'provider.script');

=item C<defaultRequester>

This is the script used to configure the requester (this machine)

It should also provide the truststore with his certificate and key (store and key password are in: requester-server-http.xml)

=cut 
has 'defaultRequester'  => (is  => 'rw', isa => 'Str', default => 'requester.script');  #

=item C<session>

Reference to the PhEDEx session

=back

=cut
has 'session'           => (is  => 'rw', isa => 'Ref');
has 'uuid'              => (is  => 'rw', isa => 'Data::UUID', default => sub {new Data::UUID});
has 'verbose'           => (is  => 'rw', isa => 'Int', default => 0);

sub BUILD {
    my $self = shift;
    $self->command("java ".$self->nsiToolJavaFlags."-jar ".$self->nsiTool);
    $self->taskManager->verbose($self->verbose);
}

=head1 METHODS

=over
 
=item C<_poe_init>

Init POE events
 
 - declare event 'processToolOutput' which is passed as a postback to the task manager
 
 - call super
 
 - launch the NSI CLI

=cut


override '_poe_init' => sub {
    my ($self, $kernel, $session) = @_;

    # Create the action which is going to be called on STDOUT by External
    $kernel->state('processToolOutput', $self);
    $self->session($session);
    $self->actionHandler($session->postback('processToolOutput'));

    super();
    
    # Launch an instance of the NSI CLI
    chdir $self->nsiToolLocation;
    $self->nsiToolPid($self->taskManager->startCommand($self->command, $self->actionHandler, $self->timeout));
};

=item C<getSetupScript>

Returns the NSI CLI script to set up the provider and requester.

Use only if you work with different providers/requesters at the same time

=cut
sub getSetupScript {
    my ($self, $providerName, $requesterName, $scriptName) = @_;
    
    $providerName = $self->defaultProvider if ! defined $providerName;
    $requesterName = $self->defaultRequester if ! defined $requesterName;
    
    if (! defined $scriptName) {
        $self->Alert("The script to load has not been provided")
    }
    
    my $script = "";
    $script .= "script --file ".$self->nsiToolLocation."/scripts/provider/$providerName\n";
    $script .= "script --file ".$self->nsiToolLocation."/scripts/requester/$requesterName\n";

    return $script;
}

=item C<backendRequestResource>

Extended method from Core::Core. Takes a request object, creates the NetworkResource, adds it to the
pending set, after which it queues the REQUEST action

=cut
override 'backendRequestResource' => sub {
    my ($self, $kernel, $session, $request) = @_[ OBJECT, KERNEL, SESSION, ARG0];

    super();

    # If we get to here it means that we can request a resource
    my $path = $self->getPathBySiteNames($request->siteA, $request->siteB, $request->bidirectional);

    # Create the resource
    my $resource = PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource->new(backendName => 'NSI', path => $path);
    $resource->setStatus('Requesting', $request->bandwidth); 
    
    # Add to pending sets
    $self->addToPending($resource);

    $self->queueThenExecuteAction(REQUEST, $resource, $request->callback);
};

=item C<backendUpdateResource>

Extended method from Core::Core. Queues an UPDATE action, although that's tot fully implemented yet

=cut
override 'backendUpdateResource' => sub {
    my ($self, $kernel, $session, $resource, $callback) = @_[ OBJECT, KERNEL, SESSION, ARG0, ARG1];
    super();
    $self->queueThenExecuteAction(UPDATE, $resource, $callback);
};

=item C<backendTeardownResource>

Extended method from Core::Core. Queues an TEARDOWN action

=cut
override 'backendTeardownResource' => sub {
    my ($self, $kernel, $session, $resource, $callback) = @_[ OBJECT, KERNEL, SESSION, ARG0, ARG1];
    super();
    $self->queueThenExecuteAction(TEARDOWN, $resource, $callback);
};

=item C<queueThenExecuteAction>

Creates a new action based on the action type provided (Request, Teardown).
This action is queued and will be executed after the rest of the pending actions have been completed.
This restriction is due to us using the NSI CLI tool instead of having a native implementation

=cut
sub queueThenExecuteAction {
    # TODO: Check if this can't be replaced by a Moose trigger on the actionQueue attribute
    my ($self, $actionType, $resource, $requestCallback) = @_;
    my $msg = 'NSI->queueThenExecuteAction';
    if (! PHEDEX::File::Download::Circuits::Helpers::Utils::Utils::checkArguments(@_)) {
        $self->Logmsg("$msg: Invalid parameters have been supplied");
        return;
    }

    my $action = PHEDEX::File::Download::Circuits::Backend::NSI::Action->new(type => $actionType, 
                                                                             resource => $resource, 
                                                                             callback => $requestCallback);
    
    $self->Logmsg("$msg: Queuing newly received action (assigned id:".$action->id.")");

    $self->queueAction($action);
    $self->executeNextAction();
}

=item C<executeNextAction>

Takes the next action in the queue and executes it if there are no other current actions taking place ATM.

=cut
# Executes the next action in the queue if there's no other pending action
sub executeNextAction {
    my $self = shift;
    my $msg = "NSI->executeNextAction";
    
    if ($self->hasCurrentAction) {
        $self->Logmsg("$msg: Currently executing action '".$self->currentAction->type."' (actionId: ".$self->currentAction->id.")");
        return;
    }

    if ($self->actionQueueSize == 0) {
        $self->Logmsg("$msg: The action queue is empty...");
        return;
    }
    
    # Pick the next action from the queue
    $self->currentAction($self->dequeueAction());
    my $action = $self->currentAction;
    
    switch ($action->type) {
        case REQUEST {
            # Create a reservation and add it to the currently executing action
            my $reservation = PHEDEX::File::Download::Circuits::Backend::NSI::Reservation->new();
            $reservation->updateParameters($action->resource);
            $action->reservation($reservation);

            # Set the reservation parameters into the CLI
            my $reserveCommands = $reservation->getReservationSetterScript();

            # And request the circuit
            push (@{$reserveCommands}, "nsi reserve\n");
            
            $self->sendToCLI($reserveCommands);
        }
        case UPDATE {
            # TODO: Implement this case
            $self->Logmsg("$msg: This action is not yet supported");
        }
        case TEARDOWN {
            # Get the reservation which was assigned to this circuit and send the CLI commands to terminate reservation
            my $connectionID = $action->resource->physicalId; 
            my $reservation = $self->getReservation($connectionID);

            if (! defined $reservation) {
                $self->Logmsg("$msg: A teardown command has been issued for a connection which does not exist");
                $self->clearCurrentAction;
                return;
            }

            my $terminationCommands = $reservation->getTerminationScript();
            $self->sendToCLI($terminationCommands);
        }
    }

    $self->executeNextAction();
}

=item C<sendToCLI>

Send a given script to the NSI CLI

=cut
sub sendToCLI {
    my ($self, $script) = @_;
    
    if (! defined $script || $script eq "") {
        $self->Logmsg("Cannot execute an empty script");
        return;
    }
    
    # Get the task info
    my $task = $self->taskManager->getTask($self->nsiToolPid)->task;

    foreach my $line (@{$script}) {
        $task->put($line);
    }
}

=item C<terminateReservation>

Terminates a reservation request in case of issues.

Sends terminate script to the CLI, removes the reservation from the hash, calls back to the 
ResourceManager to inform of a request failure, removes NetworkResource from pending and clears the current action

=cut
sub terminateReservation {
    my ($self, $reservation, $connectionId) = @_;

    # Send the termination commands for this reservation
    $self->sendToCLI($reservation->getTerminationScript());
    # Remove from the reservation list if it had been added
    $self->removeReservation($connectionId) if ($self->hasReservation($connectionId));
    # Inform the resource manager that the request failed
    $self->currentAction->callback(undef, REQUEST_FAILED);
    # Remove from pending resources 
    $self->removeFromPending($reservation->resource);
    $self->clearCurrentAction;
}

=item C<processToolOutput>

This method is called at each line out from STDOUT/ERR.

It contains all the logic needed to parse the various steps in a reservation process: AssignedId,
Confirmed, Commited, Active, Terminated.

=back

=cut
sub processToolOutput {
    my ($self, $kernel, $session, $arguments) = @_[OBJECT, KERNEL, SESSION, ARG1];
    my $msg = "NSI->processToolOutput";
    
    my $pid = $arguments->[EXTERNAL_PID];
    my $task = $arguments->[EXTERNAL_TASK];
    my $eventName = $arguments->[EXTERNAL_EVENTNAME];
    my $output = $arguments->[EXTERNAL_OUTPUT];

    switch ($eventName) {
        case 'handleTaskStdOut' {
#            $self->Logmsg("NSI CLI($pid): $output") if $self->verbose;
            
            my $connectionId = PHEDEX::File::Download::Circuits::Backend::NSI::StateMachine::isValidMessage($output);
            return if (! defined $connectionId);

            my $reservation;
            
            # Either the reservation is already known
            if ($self->hasReservation($connectionId)) {
                $reservation = $self->getReservation($connectionId);
            } else {
                # Or the reservation is new and doesn't have a connection ID yet
                if (! defined $self->currentAction->reservation->connectionId) {
                    $reservation = $self->currentAction->reservation;
                    $reservation->connectionId($connectionId);
                    $self->currentAction->resource->physicalId($connectionId);
                } else {
                    # Or we received a message about a reservation which we have no traces of
                    $self->Logmsg("Couldn't find any reservation matching this ID");
                    return;
                }
            }
            
            my $result = $reservation->stateMachine->identifyNextTransition($output);
            if (!defined $result) {
                # If we got here it means we missed a message about this reservation.
                # We terminate the reservation and inform the resource manager that it died
                $self->Logmsg("$msg: We idenfitied a reservation, but the output we got doesn't match the reservation's next logical state");
                $self->terminateReservation($reservation, $connectionId);
                return;
            };
            
            my $transition = $result->[1];
            $reservation->stateMachine->doTransition($transition);

            switch($reservation->stateMachine->currentState) {
                # The reservation initially has no ConnectionID assigned to it
                # The aggregator assigns one
                case 'AssignedId' {
                    $self->Logmsg("$msg: Circuit was assigned a connection id ($connectionId)");
                    $self->addReservation($connectionId, $reservation);
                }
                
                # If the reservation was held, then commit it
                case 'Confirmed' {
                    $self->Logmsg("$msg: Circuit was confirmed. Issuing commit next");
                    my $script = $reservation->getOverrideScript();
                    push (@{$script}, "nsi commit\n");
                    $self->sendToCLI($script);
                }
                
                # If the reservation was committed, then provision it
                case 'Commited' {
                    $self->Logmsg("$msg: Circuit was commited. Issuing provision next");
                    my $script = $reservation->getOverrideScript();
                    push (@{$script}, "nsi provision\n");
                    $self->sendToCLI($script);
                }
                
                # Reservation is now active (dataplane should now work)
                case 'Active' {
                    $self->Logmsg("$msg: Circuit is now active. Informing the resource manager");
                    # Circuit is now active
                    # - change status of the NetworkResource (handled by the ResourceManager) to "Online"
                    # - inform the ResourceManager that the request succeeded
                    # - backend needs to mark the NetworkResource as Active (from Pending)
                    my $resource = $reservation->resource;
                    $resource->setStatus("Online", $reservation->parameters->bandwidth, $reservation->connectionId);
                    $self->currentAction->callback->($resource, REQUEST_SUCCEEDED); 
                    $self->moveFromPendingToActive($resource);
                    $self->clearCurrentAction;
                }

                # Reservation has been terminated
                case 'Terminated' {
                    $self->Logmsg("$msg: Circuit terminated");
                    # Circuit is now offline
                    # - change status of the NetworkResource (handled by the ResourceManager) to "Offline"
                    # - inform the ResourceManager that the request succeeded and the resource is now offline
                    # - backend needs to remove the NetworkResource from Active set
                    my $resource = $reservation->resource;
                    $resource->setStatus("Offline");
                    $self->currentAction->callback->($resource, TERMINATE_SUCCEEDED);
                    $self->removeReservation($reservation->connectionId);
                    $self->removeFromActive($resource);
                    $self->clearCurrentAction;
                }

                # The reservation failed for whatever reason
                case ['Error', 'ConfirmFail', 'CommitFail', 'ProvisionFail'] {
                    $self->Logmsg("$msg: Circuit failed");
                    $self->terminateReservation($reservation, $connectionId);
                }
            }
        }

        case 'handleTaskStdError' {
            $self->Logmsg("An error has occured with NSI CLI ($pid): $output");
        }

        case 'handleTaskSignal' {
            $self->Logmsg("NSI CLI tool is being terminated ");
        }
    }
}

1;