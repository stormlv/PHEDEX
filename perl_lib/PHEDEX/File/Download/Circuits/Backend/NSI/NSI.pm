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

has 'taskManager'       => (is  => 'rw', isa => 'PHEDEX::File::Download::Circuits::Helpers::Tasks::TaskManager', default => sub { PHEDEX::File::Download::Circuits::Helpers::Tasks::TaskManager->new() });
has 'actionHandler'     => (is  => 'rw', isa => 'Ref');
has 'timeout'           => (is  => 'rw', isa => 'Int', default => 120);
has 'actionQueue'       => (is  => 'rw', isa => 'ArrayRef[PHEDEX::File::Download::Circuits::Backend::NSI::Action]', 
                            traits => ['Array'],
                            handles => {queueAction     => 'push', 
                                        dequeueAction   => 'shift', 
                                        actionQueueSize => 'count'});
has 'currentAction'     => (is  => 'rw', isa => 'PHEDEX::File::Download::Circuits::Backend::NSI::Action',
                            clearer     => 'clearCurrentAction',
                            predicate   => 'hasCurrentAction');
has 'reservations'      => (is  => 'rw', isa => 'HashRef[PHEDEX::File::Download::Circuits::Backend::NSI::Reservation]', 
                            traits => ['Hash'],  
                            handles => {addReservation      => 'set',
                                        getReservation      => 'get',
                                        removeReservation   => 'delete',
                                        hasReservation      => 'exists'});
has 'nsiToolLocation'   => (is  => 'rw', isa => 'Str', default => '/data/NSI/CLI');
has 'nsiTool'           => (is  => 'rw', isa => 'Str', default => 'nsi-cli-1.2.1-one-jar.jar');
has 'nsiToolJavaFlags'  => (is  => 'rw', isa => 'Str', default =>   '-Xmx256m -Djava.net.preferIPv4Stack=true '.
                                                                    '-Dlog4j.configuration=file:./config/log4j.properties '.
                                                                    '-Dcom.sun.xml.bind.v2.runtime.JAXBContextImpl.fastBoot=true '.
                                                                    '-Dorg.apache.cxf.JDKBugHacks.defaultUsesCaches=true ');
has 'nsiToolPid'        => (is  => 'rw', isa => 'Int');
has 'command'           => (is  => 'rw', isa => 'Str');
has 'defaultProvider'   => (is  => 'rw', isa => 'Str', default => 'provider.script');   # Provider should also have the truststore containing the aggregator server certificats (store password is in: provider-client-https-cc.xml)
has 'defaultRequester'  => (is  => 'rw', isa => 'Str', default => 'requester.script');  # Requester should also provide the truststore with his certificate and key (store and key password are in: requester-server-http.xml)
has 'session'           => (is  => 'rw', isa => 'Ref');
has 'uuid'              => (is  => 'rw', isa => 'Data::UUID', default => sub {new Data::UUID});
has 'verbose'           => (is  => 'rw', isa => 'Int', default => 0);

sub BUILD {
    my $self = shift;
    $self->command("java ".$self->nsiToolJavaFlags."-jar ".$self->nsiTool);
    $self->taskManager->verbose($self->verbose);
}

# Init POE events
# - declare event 'processToolOutput' which is passed as a postback to External
# - call super
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

sub getRequestScript {
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

override 'backendUpdateResource' => sub {
    my ($self, $kernel, $session, $resource, $callback) = @_[ OBJECT, KERNEL, SESSION, ARG0, ARG1];
    super();
    $self->queueThenExecuteAction(UPDATE, $resource, $callback);
};

override 'backendTeardownResource' => sub {
    my ($self, $kernel, $session, $resource, $callback) = @_[ OBJECT, KERNEL, SESSION, ARG0, ARG1];
    super();
    $self->queueThenExecuteAction(TEARDOWN, $resource, $callback);
};

# Creates a new action based on the action type provided (Request, Teardown)
# This action is queued and will be executed after the rest of the pending actions have been completed
# This restriction is due to us using the NSI CLI tool instead of having a native implementation
sub queueThenExecuteAction {
    # TODO: Check if this can't be replaced by a Moose trigger on the actionQueue attribute
    my ($self, $actionType, $resource, $requestCallback) = @_;
    my $msg = 'NSI->queueThenExecuteAction';
    if (! PHEDEX::File::Download::Circuits::Helpers::Utils::Utils::checkArguments(@_)) {
        $self->Logmsg("$msg: Invalid parameters have been supplied");
        return;
    }

    my $actionId = $self->uuid->to_string($self->uuid->create());

    my $action = PHEDEX::File::Download::Circuits::Backend::NSI::Action->new(id => $actionId, 
                                                                             type => $actionType, 
                                                                             resource => $resource, 
                                                                             callback => $requestCallback);
    
    $self->Logmsg("$msg: Queuing newly received action (assigned id: $actionId");

    $self->queueAction($action);
    $self->executeNextAction();
}

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

# Send commands to the NSI CLI
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