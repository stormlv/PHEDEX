package PHEDEX::Tests::File::Download::TestCircuit;

use strict;
use warnings;

use IO::File;
use File::Path;
use PHEDEX::File::Download::Circuits::Circuit;
use PHEDEX::File::Download::Circuits::Constants;
use PHEDEX::Tests::File::Download::Helpers::ObjectCreation;
use PHEDEX::Core::Timing;
use Test::More;

# Trivial test consists of creating a circuit and making sure parameters are initialized correctly
sub testObjectInitialisation {
    my $testCircuit = PHEDEX::File::Download::Circuits::Circuit->new(PHEDEX_FROM_NODE => 'T2_ANSE_GENEVA',
                                                                     PHEDEX_TO_NODE => 'T2_ANSE_AMSTERDAM',
                                                                     CIRCUITDIR => '/tmp2/circuit');

    ok($testCircuit->{ID}, 'circuit / testObjectInitialisation - ID ok');
    is($testCircuit->{PHEDEX_FROM_NODE}, 'T2_ANSE_GENEVA', 'circuit / testObjectInitialisation - PHEDEX_FROM_NODE ok');
    is($testCircuit->{PHEDEX_TO_NODE}, 'T2_ANSE_AMSTERDAM', 'circuit / testObjectInitialisation - PHEDEX_TI_NODE ok');
    is($testCircuit->{BOOKING_BACKEND}, 'Dummy', 'circuit / testObjectInitialisation - BOOKING_BACKEND ok');
    is($testCircuit->{SCOPE}, 'GENERIC', 'circuit / testObjectInitialisation - SCOPE ok');
    is($testCircuit->{CIRCUITDIR}, '/tmp2/circuit', 'circuit / testObjectInitialisation - CIRCUITDIR ok');
    is($testCircuit->{CIRCUIT_REQUEST_TIMEOUT}, 5*MINUTE, 'circuit / testObjectInitialisation - CIRCUIT_REQUEST_TIMEOUT ok');
    is($testCircuit->{CIRCUIT_DEFAULT_LIFETIME}, 5*HOUR, 'circuit / testObjectInitialisation - CIRCUIT_DEFAULT_LIFETIME ok');
}

# Testing the getExpirationTime, isExpired and getLinkName subroutines
sub testHelperMethods {
    # Test getExpiration time and isExpired
    my ($time, $timeNow) = (479317500, &mytimeofday() - 1800);

    my $testCircuit1 = createRequestingCircuit($time, 'Dummy', 'T2_ANSE_CERN_1', 'T2_ANSE_CERN_2');
    my $testCircuit2 = createEstablishedCircuit($time, '192.168.0.1', '192.168.0.2', undef, $time, 'Dummy', 'T2_ANSE_CERN_1', 'T2_ANSE_CERN_2');
    my $testCircuit3 = createEstablishedCircuit($time, '192.168.0.1', '192.168.0.2', undef, $time, 'Dummy', 'T2_ANSE_CERN_1', 'T2_ANSE_CERN_2', 1800);
    my $testCircuit4 = createEstablishedCircuit($timeNow, '192.168.0.1', '192.168.0.2', undef, $timeNow, 'Dummy', 'T2_ANSE_CERN_1', 'T2_ANSE_CERN_2', 1700);
    my $testCircuit5 = createEstablishedCircuit($timeNow, '192.168.0.1', '192.168.0.2', undef, $timeNow, 'Dummy', 'T2_ANSE_CERN_1', 'T2_ANSE_CERN_2', 1900);
    my $testCircuit6 = createOfflineCircuit();

    # Test getExpirationTime
    is($testCircuit1->getExpirationTime(), undef, "circuit / testHelperMethods - Cannot get expiration time on a requesting circuit");
    is($testCircuit2->getExpirationTime(), undef, "circuit / testHelperMethods - Cannot get expiration time on a circuit without a lifetime set");
    is($testCircuit3->getExpirationTime(), 479317500+1800, "circuit / testHelperMethods - Expiration time correctly estimated");

    # Test isExpired
    is($testCircuit1->isExpired(), 0, "circuit / testHelperMethods - isExpired on a requesting circuit returns 0");
    is($testCircuit2->isExpired(), 0, "circuit / testHelperMethods - isExpired on an established circuit without lifetime returns 0");
    is($testCircuit3->isExpired(), 1, "circuit / testHelperMethods - Established circuit expired");
    is($testCircuit4->isExpired(), 1, "circuit / testHelperMethods - Established circuit expired");
    is($testCircuit5->isExpired(), 0, "circuit / testHelperMethods - Established circuit still valid");

    # Test getLinkName
    is($testCircuit1->getLinkName(), 'T2_ANSE_CERN_1-to-T2_ANSE_CERN_2', "circuit / testHelperMethods - getLinkName works as it should");

    # Test getSaveName
    ok($testCircuit1->getSaveName() =~ /requested/, "circuit / testHelperMethods - getSaveName works as it should on a requested circuit");
    ok($testCircuit2->getSaveName() =~ /online/, "circuit / testHelperMethods - getSaveName works as it should on a established circuit");
    ok($testCircuit6->getSaveName() =~ /offline/, "circuit / testHelperMethods - getSaveName works as it should on a offline circuit");
}

# Test consists of creating a circuit then attempting to save it
# while it is in different states. saveState should detect inconsistencies or errors
# and not attempt any save
sub testSaveErrorHandling {

    #Clean tmp dir before everything else
    File::Path::rmtree('/tmp/circuit', 1, 1) if (-d '/tmp/circuit');
    my $testCircuit = PHEDEX::File::Download::Circuits::Circuit->new();

    is($testCircuit->saveState(), CIRCUIT_ERROR_SAVING, 'circuit / testSaveErrorHandling - Unable to save since status is not saved');
    $testCircuit->{STATUS} = CIRCUIT_STATUS_REQUESTING;

    is($testCircuit->saveState(), CIRCUIT_ERROR_SAVING, 'circuit / testSaveErrorHandling - Unable to save since request time is not set');
    $testCircuit->{REQUEST_TIME} = 1398426904;

    $testCircuit->{PHEDEX_TO_NODE} = 'T2_ANSE_AMSTERDAM';
    $testCircuit->{PHEDEX_FROM_NODE} = 'T2_ANSE_GENEVA';

    $testCircuit->{CIRCUITDIR} = '';
    is($testCircuit->saveState(), CIRCUIT_ERROR_SAVING, 'circuit / testSaveErrorHandling - Unable to save since circuit cannot create state folder');

    File::Path::make_path('/tmp/circuit');
    File::Path::make_path('/tmp/circuit/requested', { mode => 0555 });
    $testCircuit->{CIRCUITDIR} = '/tmp/circuit';
    is($testCircuit->saveState(), CIRCUIT_ERROR_SAVING, 'circuit / testSaveErrorHandling - Unable to save since circuit cannot write to state folder');

    # Because rmtree doesn't seem to work when w permissions are missing
    system "rm -rf /tmp/circuit";
}

# Trivial test consisting of trying to open invalid circuits
sub testOpenErrorHandling {
    is(PHEDEX::File::Download::Circuits::Circuit->openCircuit(undef), CIRCUIT_ERROR_OPENING, 'circuit / testOpenErrorHandling - Unable to open since path is not defined');
    is(PHEDEX::File::Download::Circuits::Circuit->openCircuit('/tmp/bla.circuit'), CIRCUIT_ERROR_OPENING, 'circuit / testOpenErrorHandling - Unable to open since the path is invalid');
    my $fh = new IO::File "/tmp/bla.circuit";
    if (defined $fh) {
        print $fh "This is malformed file\n";
        $fh->close();
    }
    is(PHEDEX::File::Download::Circuits::Circuit->openCircuit('/tmp/bla.circuit'), CIRCUIT_ERROR_OPENING, 'circuit / testOpenErrorHandling - Unable to open an invalid circuit');
}

# Test consists of creating circuits, saving them, reopening then verifying that the two match
sub testSaveOpenObject{
    File::Path::rmtree('/tmp/circuit', 1, 1) if (-d '/tmp/circuit');

    # save a circuit which is 'in request'
    my $testCircuit1 = createRequestingCircuit();
    my $saveName1 = $testCircuit1->getSaveName();

    is($testCircuit1->saveState(), CIRCUIT_OK, 'circuit / testSaveOpenObject - Successfully saved circuit in request state');
    ok(-e $saveName1, 'circuit / testSaveOpenObject - Tested that save file exists');

    my ($openedCircuit, $code) = PHEDEX::File::Download::Circuits::Circuit::openCircuit($saveName1);
    is($code, CIRCUIT_OK, 'circuit / testSaveOpenObject - Opened previously saved circuit in request state');
    is_deeply($testCircuit1, $openedCircuit, "circuit / testSaveOpenObject - Opened circuit is identical to the one which was saved");

    # save a circuit which is 'established'
    my $testCircuit2 = createEstablishedCircuit();
    my $saveName2 = $testCircuit2->getSaveName();

    is($testCircuit2->saveState(), CIRCUIT_OK, 'circuit / testSaveOpenObject - Successfully saved circuit in established state');
    ok(-e $saveName2, 'circuit / testSaveOpenObject - Tested that save file exists');

    my ($openedCircuit2, $code2) = PHEDEX::File::Download::Circuits::Circuit::openCircuit($saveName2);
    is($code2, CIRCUIT_OK, 'circuit / testSaveOpenObject - Opened previously saved circuit in established state');
    is_deeply($testCircuit2, $openedCircuit2, "circuit / testSaveOpenObject - Opened circuit is identical to the one which was saved");

    my $testCircuit3 = createEstablishedCircuit();
    $testCircuit3->registerTakeDown();

    my $time = formattedTime($testCircuit3->{LAST_STATUS_CHANGE});
    is($testCircuit3->saveState(), CIRCUIT_OK, 'circuit / testSaveOpenObject - Successfully saved circuit in offline state');

    ok(-e $testCircuit3->getSaveName(), 'circuit / testSaveOpenObject - Tested that save file exists');
}

# Test consists of creating circuits, saving them then calling removeState on them (which should remove them from disk)
sub testRemoveStateFiles {
     File::Path::rmtree('/tmp/circuit', 1, 1) if (-d '/tmp/circuit');

    # remove a state from a circuit which is 'in request'
    my $testCircuit = createRequestingCircuit();
    $testCircuit->saveState();
    ok(-e $testCircuit->getSaveName(), 'circuit / testRemoveStateFiles - State file exists in /requested');
    is($testCircuit->removeState(), CIRCUIT_OK, 'circuit / testRemoveStateFiles - State file removed from /requested');
    is(-e $testCircuit->getSaveName(), undef, 'circuit / testRemoveStateFiles - State file no longer exists in /requested');

    # remove a state from a circuit which is 'established'
    my $testCircuit2 = createEstablishedCircuit();
    $testCircuit2->saveState();
    ok(-e $testCircuit2->getSaveName(), 'circuit / testRemoveStateFiles - State file exists in /established');
    is($testCircuit2->removeState(), CIRCUIT_OK, 'circuit / testRemoveStateFiles - State file removed from /established');
    is(-e $testCircuit2->getSaveName(), undef, 'circuit / testRemoveStateFiles - State file no longer exists in /established');

    # remove a state from a circuit which is "offline"
    my $testCircuit3 = createEstablishedCircuit();
    $testCircuit3->registerTakeDown();
    $testCircuit3->saveState();
    ok(-e $testCircuit3->getSaveName(), 'circuit / testRemoveStateFiles - State file exists in /offline');
    is($testCircuit3->removeState(), CIRCUIT_OK, 'circuit / testRemoveStateFiles - State file removed from /offline');
    is(-e $testCircuit3->getSaveName(), undef, 'circuit / testRemoveStateFiles - State file no longer exists in /offline');

    # attempt (and fail) to remove a state for which there's no file
    my $testCircuit4 = createEstablishedCircuit();
    $testCircuit4->saveState();
    unlink $testCircuit4->getSaveName();
    is($testCircuit4->removeState(), CIRCUIT_GENERIC_ERROR, 'circuit / testRemoveStateFiles - cannot find saved state');
}

# Test consists of creating circuits, then attempting to change their statuses
# by calling one of the subroutines registerRequest, registerEstablished, registerRequestFailure
# Circuit should only allow switching when certain parameters are met; for ex. cannot call registerEstablished on
# a circuit which isn't in request mode
sub testStatusChange{
    # Test methods on an empty circuit
    my $emptyCircuit = PHEDEX::File::Download::Circuits::Circuit->new();
    is($emptyCircuit->registerRequest(), CIRCUIT_GENERIC_ERROR, "circuit / testStatusChange - Cannot change state to requested");
    is($emptyCircuit->registerEstablished(), CIRCUIT_GENERIC_ERROR, "circuit / testStatusChange - Cannot change state to established");

    # Test methods on an "offline" circuit
    my $testCircuit1 = PHEDEX::File::Download::Circuits::Circuit->new(PHEDEX_FROM_NODE => 'T2_ANSE_GENEVA', PHEDEX_TO_NODE => 'T2_ANSE_AMSTERDAM');
    is($testCircuit1->registerRequest(), CIRCUIT_GENERIC_ERROR, "circuit / testStatusChange - Cannot change state to requested");
    is($testCircuit1->registerRequest('Dummy'), CIRCUIT_OK, "circuit / testStatusChange - Changed state to requested");
    is($testCircuit1->{STATUS}, CIRCUIT_STATUS_REQUESTING, "circuit / testStatusChange - Changed state verified");
    is($testCircuit1->registerEstablished(), CIRCUIT_GENERIC_ERROR, "circuit / testStatusChange - Cannot change state to established");

    # Test methods on a circuit which is "in request"
    my $testCircuit2 = createRequestingCircuit();
    is($testCircuit2->registerRequest(), CIRCUIT_GENERIC_ERROR, "circuit / testStatusChange - Cannot change state to requested");
    is($testCircuit2->registerEstablished('192.168.0.1'), CIRCUIT_GENERIC_ERROR, "circuit / testStatusChange - Cannot change stat to established");
    is($testCircuit2->registerEstablished('192.168.0.1', '256.168.0.1'), CIRCUIT_GENERIC_ERROR, "circuit / testStatusChange - Cannot change stat to established");
    is($testCircuit2->registerEstablished('192.168.0.1','192.168.0.2'), CIRCUIT_OK, "circuit / testStatusChange - Changed state to established");
    is($testCircuit2->{STATUS}, CIRCUIT_STATUS_ONLINE, "circuit / testStatusChange - Changed state verified - STATUS set");
    is($testCircuit2->{CIRCUIT_FROM_IP}, '192.168.0.1', "circuit / testStatusChange - Changed state verified - fromIP set");
    is($testCircuit2->{CIRCUIT_TO_IP}, '192.168.0.2', "circuit / testStatusChange - Changed state verified - toIP set");

    # Test methods on a circuit which is "in request"
    my $testCircuit3 = createRequestingCircuit();
    is($testCircuit3->registerRequestFailure('my reason'), CIRCUIT_OK, "circuit / testStatusChange - Changed state to request failed");
    is($testCircuit3->{STATUS}, CIRCUIT_STATUS_OFFLINE, "circuit / testStatusChange - Changed state verified - STATUS set");
}

# Test consists of checking that registerRequestFailure and registerTransferFailure correctly save error details in circuit
sub testErrorLogging {
    #Request error
    my $testCircuit1 = createRequestingCircuit();
    is($testCircuit1->registerRequestFailure('my reason'), CIRCUIT_OK, "circuit / testErrorLogging - Changed state to request failed");
    is($testCircuit1->getFailedRequest()->[0], $testCircuit1->{LAST_STATUS_CHANGE}, "circuit / testErrorLogging - Failed request successfully logged");
    is($testCircuit1->getFailedRequest()->[1], 'my reason', "circuit / testErrorLogging - Failed request successfully logged");

    my $startTime = &mytimeofday();
    # my ($startTime, $size, $jobsize, $jobDuration) = @_;
    # Transfer errors
    my $testCircuit2 = createRequestingCircuit();
    my $task1 = createTask($startTime, 1024**3, 30, 30);
    my $task2 = createTask($startTime, 1024**3, 30, 30);

    is($testCircuit2->registerEstablished('192.168.0.1','192.168.0.2', 'Dummy'), CIRCUIT_OK, "circuit / testErrorLogging - Changed state to established");
    is($testCircuit2->registerTransferFailure($task1, 'failure 1'), CIRCUIT_OK, "circuit / testErrorLogging - Task failure registered");
    is($testCircuit2->registerTransferFailure($task2, 'failure 2'), CIRCUIT_OK, "circuit / testErrorLogging - Task failure registered");

    my $failedTransfers = $testCircuit2->getFailedTransfers();

    is(scalar @$failedTransfers, 2, 'circuit / testErrorLogging - Two transfers failed');

    is_deeply($task1, $failedTransfers->[0][1], "circuit / testErrorLogging - Initial circuit and opened circuit do not differ");
    is_deeply($task2, $failedTransfers->[1][1], "circuit / testErrorLogging - Initial circuit and opened circuit do not differ");
}

# Test of the circuit comparison
sub testCircuitComparison {
    my ($proto, $testCircuit1) = (createEstablishedCircuit(), createEstablishedCircuit());

    is($proto->compareCircuits(undef), 0, "circuit / testCircuitComparison - Compare with undef fails");
    is(compareCircuits($proto, $proto), 1, "circuit / testCircuitComparison - Compare with self is ok");
    is(compareCircuits($proto, $testCircuit1), 0, "circuit / testCircuitComparison - Compare fails for circuit with different ID ");

    $testCircuit1->{ID} = $proto->{ID};
    $testCircuit1->{LAST_STATUS_CHANGE} = $proto->{LAST_STATUS_CHANGE};
    is(compareCircuits($proto, $testCircuit1), 1, "circuit / testCircuitComparison - Compare succeeds for identical simple circuits");

    my $startTime = &mytimeofday();
    my $task1 = createTask($startTime, 1024**3, 30, 30);
    my $task2 = createTask($startTime, 1024**3, 30, 30);
    my $task3 = createTask($startTime, 1024**3, 30, 30);
    $testCircuit1->registerTransferFailure($task1, 'failure 1');
    $testCircuit1->registerTransferFailure($task2, 'failure 2');
    is(compareCircuits($proto, $testCircuit1), 0, "circuit / testCircuitComparison - Compare fails for different more complex circuits");

    $proto->registerTransferFailure($task1, 'failure 1');
    $proto->registerTransferFailure($task2, 'failure 2');
    is(compareCircuits($proto, $testCircuit1), 0, "circuit / testCircuitComparison - Compare still fails since the array items are not the same");

    $proto->{FAILURES}{CIRCUIT_FAILED_TRANSFERS}->[0][0] = $testCircuit1->{FAILURES}{CIRCUIT_FAILED_TRANSFERS}->[0][0];
    $proto->{FAILURES}{CIRCUIT_FAILED_TRANSFERS}->[1][0] = $testCircuit1->{FAILURES}{CIRCUIT_FAILED_TRANSFERS}->[1][0];
    is(compareCircuits($proto, $testCircuit1), 1, "circuit / testCircuitComparison - Compare succeeds for complex circuits");

    $proto->{FAILURES}{CIRCUIT_FAILED_TRANSFERS}->[0][1] = $task3;
    is(compareCircuits($proto, $testCircuit1), 0, "circuit / testCircuitComparison - Compare succeeds for complex circuits");

    $proto->{FAILURES}{CIRCUIT_FAILED_TRANSFERS}->[0][1]->{TASKID} = 1;
    is(compareCircuits($proto, $testCircuit1), 0, "circuit / testCircuitComparison - Compare fails when task id is modified");
}

testObjectInitialisation();
testHelperMethods();
testSaveErrorHandling();
testOpenErrorHandling();
testSaveOpenObject();
testRemoveStateFiles();
testStatusChange();
testErrorLogging();
testCircuitComparison();

done_testing();

1;