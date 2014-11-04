package PHEDEX::Tests::File::Download::TestNormalFunctionality;

use strict;
use warnings;


use File::Copy qw(move);
use IO::File;
use POE;
use POSIX;
use Test::More;

use PHEDEX::Core::Timing;
use PHEDEX::File::Download::Circuits::Constants;
use PHEDEX::File::Download::Circuits::ManagedResource::Circuit;
use PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource;
use PHEDEX::File::Download::Circuits::ResourceManager;
use PHEDEX::Tests::File::Download::Helpers::ObjectCreation;
use PHEDEX::Tests::File::Download::Helpers::SessionCreation;


# Tests the various smaller subroutines (checkCircuit, canRequestCircuit) from the circuit manager
sub testHelperMethods {
    my $circuitManager = PHEDEX::File::Download::Circuits::ResourceManager->new(BACKEND_TYPE => 'Dummy',
                                                                                BACKEND_ARGS => {AGENT_TRANSLATION_FILE => '/data/agent_ips.txt'},
                                                                                CIRCUITDIR => "$baseLocation".'/data',
                                                                                VERBOSE => 1);
    $circuitManager->Logmsg('Testing helper methods');

    my $time = &mytimeofday();

    # testing checkCircuit
    my $requestingCircuit = createRequestingCircuit($time, 'Dummy', 'T2_ANSE_CERN_1', 'T2_ANSE_CERN_2');
    $circuitManager->{CIRCUITS}{$requestingCircuit->{NAME}} = $requestingCircuit;

    my $establishedCircuit = createEstablishedCircuit($time, '192.168.0.1', '192.168.0.2', undef, $time, 'Dummy', 'T2_ANSE_CERN_2', 'T2_ANSE_CERN_1');
    $circuitManager->{CIRCUITS}{$establishedCircuit->{NAME}} = $establishedCircuit;

    is_deeply($circuitManager->checkCircuit('T2_ANSE_CERN_1', 'T2_ANSE_CERN_2', STATUS_CIRCUIT_REQUESTING), $requestingCircuit ,  'TestNormalFunctionality->check helper methods: checkCircuit works correctly');
    ok(!$circuitManager->checkCircuit('T2_ANSE_CERN_1', 'T2_ANSE_CERN_2', STATUS_CIRCUIT_ONLINE), 'TestNormalFunctionality->check helper methods: checkCircuit works correctly');
    is_deeply($circuitManager->checkCircuit('T2_ANSE_CERN_2', 'T2_ANSE_CERN_1', STATUS_CIRCUIT_ONLINE), $establishedCircuit ,  'TestNormalFunctionality->check helper methods: checkCircuit works correctly');
    ok(!$circuitManager->checkCircuit('T2_ANSE_CERN_2', 'T2_ANSE_CERN_1', STATUS_CIRCUIT_REQUESTING), 'TestNormalFunctionality->check helper methods: checkCircuit works correctly');

    # testing canRequestCircuit
    $circuitManager->{LINKS_BLACKLISTED}{'T2_ANSE_CERN_2-to-T2_ANSE_CERN_Dev'} = 'data';
    is($circuitManager->canRequestCircuit('T2_ANSE_CERN_1', 'T2_ANSE_CERN_Dev'), CIRCUIT_AVAILABLE, 'TestNormalFunctionality->check helper methods: canRequestCircuit says we can request a circuit');
    is($circuitManager->canRequestCircuit('T2_ANSE_CERN_1', 'T2_ANSE_CERN_2'), CIRCUIT_ALREADY_REQUESTED, 'TestNormalFunctionality->check helper methods: canRequestCircuit says we cannot request a circuit which is already in CIRCUITS');
    is($circuitManager->canRequestCircuit('T2_ANSE_CERN_2', 'T2_ANSE_CERN_Dev'), CIRCUIT_BLACKLISTED, 'TestNormalFunctionality->check helper methods: canRequestCircuit says we cannot request a circuit on a blacklisted link');
    is($circuitManager->canRequestCircuit('T2_ANSE_CERN_3', 'T2_ANSE_CERN_Dev'), CIRCUIT_UNAVAILABLE, 'TestNormalFunctionality->check helper methods: canRequestCircuit says backend cannot request a circuit on provided link');
}

# Test consists of creating 3 malformed circuits in each of the 3 locations in /circuits
# that the circuit manager should remove
sub testVSCMalformedCircuits {

    ### Setup circuit manager
    my ($circuitManager, $session) = setupResourceManager(0.1, 'malformed-circuits.log', HOUR);
    $circuitManager->Logmsg('Testing event verifyStateConsistency / removal of malformed circuits');

    ### Setup malformed circuits
    my $locations = ['requested', 'online', 'offline'];
        foreach my $location (@{$locations}) {
            File::Path::make_path("$baseLocation"."/data/circuits/$location", {error => \my $err});
            my $fh = new IO::File "> $baseLocation"."/data/circuits/$location/malformed_circuit";
            if (defined $fh) {
                print $fh "This is malformed file\n";
                $fh->close();
            }
        }

    ### Run POE
    POE::Kernel->run();

    ### The three malformed circuits should be removed
    ok(!-e '$baseLocation"."/data/circuits/requested/malformed_circuit', "TestNormalFunctionality->verifyStateConsistency - malformed file in /requested was removed");
    ok(!-e '$baseLocation"."/data/circuits/online/malformed_circuit', "TestNormalFunctionality->verifyStateConsistency - malformed file in /online was removed");
    ok(!-e '$baseLocation"."/data/circuits/offline/malformed_circuit', "TestNormalFunctionality->verifyStateConsistency - malformed file in /offline was removed");

    $circuitManager->Logmsg('Testing event verifyStateConsistency / removal of malformed circuits');
}

# Test consists of creating 3 circuits in each of the 3 locations in /circuits
# that the circuit manager should move to their correct locations
sub testVSCMisplacedCircuits {

    ### Setup circuit manager
    my ($circuitManager, $session) = setupResourceManager(0.1, 'misplaced-circuits.log', HOUR);
    $circuitManager->Logmsg('Testing event verifyStateConsistency / relocation of misplaced circuits circuits');

    my $time = &mytimeofday();

    ### Prepare misplaced circuits
    # Save and move requested circuit to online
    my $misplacedRequest = createRequestingCircuit($time, 'WDummy', 'T2_ANSE_CERN_1', 'T2_ANSE_CERN_2');
    $misplacedRequest->{STATE_DIR} = "$baseLocation".'/data/circuits';
    $misplacedRequest->saveState();
    my $fileReq = $misplacedRequest->{NAME}.'-'.formattedTime($time);
    move "$baseLocation"."/data/circuits/requested/$fileReq", "$baseLocation"."/data/circuits/online/$fileReq".'req.moved';
    ok(!-e "$baseLocation"."/data/circuits/requested/$fileReq", "TestNormalFunctionality->verifyStateConsistency - moved requested circuit from its folder");

    # Save and move establised circuit to offline
    my $misplacedEstablished = createEstablishedCircuit($time, '192.168.0.1', '192.168.0.2', undef, $time, 'WDummy', 'T2_ANSE_CERN_2', 'T2_ANSE_CERN_1');
    $misplacedEstablished->{STATE_DIR} = "$baseLocation".'/data/circuits';
    $misplacedEstablished->saveState();
    my $fileEst = $misplacedEstablished->{NAME}.'-'.formattedTime($time);
    move "$baseLocation"."/data/circuits/online/$fileEst", "$baseLocation"."/data/circuits/offline/$fileEst".'est.moved';
    ok(!-e "$baseLocation"."/data/circuits/online/$fileEst", "TestNormalFunctionality->verifyStateConsistency - moved online circuit from its folder");

    # Save and move offline circuit to online
    my $misplacedOffline = createOfflineCircuit($time);
    $misplacedOffline->{BOOKING_BACKEND} = 'WDummy';
    $misplacedOffline->{STATE_DIR} = "$baseLocation".'/data/circuits';
    $misplacedOffline->saveState();
    my $fileOff = $misplacedOffline->{NAME}.'-'.formattedTime($time);
    move "$baseLocation"."/data/circuits/offline/$fileOff", "$baseLocation"."/data/circuits/requested/$fileOff".'off.moved';
    ok(!-e "$baseLocation"."/data/circuits/offline/$fileOff", "TestNormalFunctionality->verifyStateConsistency - moved offline circuit from its folder");

    ### Run POE
    POE::Kernel->run();

    ### The three misplaced circuits should now be in their correct folders
    ok(-e $misplacedRequest->getSaveName(), "TestNormalFunctionality->verifyStateConsistency - misplaced requested circuit back in its folder");
    ok(-e $misplacedEstablished->getSaveName(), "TestNormalFunctionality->verifyStateConsistency - misplaced established circuit back in its folder");
    ok(-e $misplacedOffline->getSaveName(), "TestNormalFunctionality->verifyStateConsistency - misplaced offline circuit back in its folder");
}

# Test consists of creating usable circuits which cannot be handled by the circuit manager
# It should skip a circuit if it finds that either it doesn't share it's SCOPE, BACKEND or if the
# backend cannot create circuits for a given link anymore
sub testVSCUnclaimedCircuits {

    ### Setup circuit manager
    my ($circuitManager, $session) = setupResourceManager(0.1, 'unclaimed-circuits.log', HOUR);
    $circuitManager->Logmsg('Testing event verifyStateConsistency / skipping of circuits which don\'t relate to *this* circuit manager');

    my $time = &mytimeofday();

    my $wrongScopeCircuit = createEstablishedCircuit($time, '192.168.0.1', '192.168.0.2', undef, $time, 'Dummy', 'T2_ANSE_CERN_1', 'T2_ANSE_CERN_2');
    $wrongScopeCircuit->{SCOPE} = 'UNGENERIC';
    $wrongScopeCircuit->{STATE_DIR} = "$baseLocation".'/data/circuits';
    $wrongScopeCircuit->saveState();

    my $wrongBackendCircuit = createEstablishedCircuit($time, '192.168.0.1', '192.168.0.2', undef, $time, 'WrongDummy', 'T2_ANSE_CERN_2', 'T2_ANSE_CERN_1');
    $wrongBackendCircuit->{STATE_DIR} = "$baseLocation".'/data/circuits';
    $wrongBackendCircuit->saveState();

    my $deprecatedLinksCircuit = createEstablishedCircuit($time, '192.168.0.1', '192.168.0.2', undef, $time, 'Dummy', 'T2_ANSE_CERN_3', 'T2_ANSE_CERN_4');
    $deprecatedLinksCircuit->{STATE_DIR} = "$baseLocation".'/data/circuits';
    $deprecatedLinksCircuit->saveState();

    ### Run POE
    POE::Kernel->run();

    ok(!keys %{$circuitManager->{CIRCUITS}}, "TestNormalFunctionality->verifyStateConsistency - unclaimed circuits were not used in the backend");
}

# Test consists of creating a circuit then adding it in memory
# The circuit manager should skip it...
sub testVSCSkipIdenticalCircuits {

    ### Setup circuit manager
    my ($circuitManager, $session) = setupResourceManager(0.1, 'existing-circuits-in-memory.log', HOUR);
    $circuitManager->Logmsg('Testing event verifyStateConsistency / skipping of circuits which are already in memory');

    my $time = &mytimeofday();

    my $existingCircuit = createEstablishedCircuit($time, '192.168.0.1', '192.168.0.2', undef, $time, 'Dummy', 'T2_ANSE_CERN_1', 'T2_ANSE_CERN_2');
    $existingCircuit->{STATE_DIR} = "$baseLocation".'/data/circuits';
    $existingCircuit->saveState();

    $circuitManager->{CIRCUITS}{$existingCircuit->{NAME}} = $existingCircuit;

    ### Run POE
    POE::Kernel->run();

    my $found = logChecking('existing-circuits-in-memory.log', 'Skipping identical in-memory circuit');

    ok($found, "TestNormalFunctionality->verifyStateConsistency - circuit was skipped since it was already in memory ");
}

# Test consists of creating a circuit on disk, then creating  a different circuit in memory, both of which regard the same link
# The circuit manager should give priority to the information in memory and remove the one on disk
sub testVSCRemoveSimilarCircuits {

    ### Setup circuit manager
    my ($circuitManager, $session) = setupResourceManager(0.1, 'similar-circuits-disk-vs-memory.log', HOUR);
    $circuitManager->Logmsg('Testing event verifyStateConsistency / skipping of circuits which are already in memory');

    my $time = &mytimeofday();

    my $onDiskCircuit = createEstablishedCircuit($time, '192.168.0.1', '192.168.0.2', undef, $time, 'Dummy', 'T2_ANSE_CERN_1', 'T2_ANSE_CERN_2');
    $onDiskCircuit->{STATE_DIR} = "$baseLocation".'/data/circuits';
    $onDiskCircuit->saveState();

    my $inMemoryCircuit = createEstablishedCircuit($time - 10, '192.168.0.1', '192.168.0.2', undef, $time - 10, 'Dummy', 'T2_ANSE_CERN_1', 'T2_ANSE_CERN_2');
    $inMemoryCircuit->{STATE_DIR} = "$baseLocation".'/data/circuits';

    $circuitManager->{CIRCUITS}{$inMemoryCircuit->{NAME}} = $inMemoryCircuit;

    ### Run POE
    POE::Kernel->run();

    ok(!-e $onDiskCircuit->getSaveName(), "TestNormalFunctionality->verifyStateConsistency - similar circuit previously on disk has been removed");
    ok(-e $inMemoryCircuit->getSaveName(), "TestNormalFunctionality->verifyStateConsistency - similar circuit previously in memory has been resaved");
    is($circuitManager->{CIRCUITS}{$inMemoryCircuit->{NAME}}{ID}, $inMemoryCircuit->{ID} , "TestNormalFunctionality->verifyStateConsistency - similar circuit previously in memory has not changed");
}

# Test consists of creating a circuit request, then saving it on disk.
# The circuit manager should flag it as failed, remove previous state and save the new one in /offline
# See more in code (in circuit manager) about why we handle it like this
sub testVSCHandleCircuitRequest {

    ### Setup circuit manager
    my ($circuitManager, $session) = setupResourceManager(0.1, 'handle-circuit-request.log', HOUR);
    $circuitManager->Logmsg('Testing event verifyStateConsistency / handling newly found requests on disk');

    my $time = &mytimeofday();

    my $request = createRequestingCircuit($time, 'Dummy', 'T2_ANSE_CERN_1', 'T2_ANSE_CERN_2');
    $request->{STATE_DIR} = "$baseLocation".'/data/circuits';
    $request->saveState();

    ### Run POE
    POE::Kernel->run();

    ok(!-e $request->getSaveName(), "TestNormalFunctionality->verifyStateConsistency - circuit request no longer in /requested");

    my $partialID = substr($request->{ID}, 1, 8);

    my $file ="$baseLocation".'/data/circuits/offline/'.$request->{NAME}."-$partialID-".formattedTime($time);
    ok(-e $file, "TestNormalFunctionality->verifyStateConsistency - circuit request marked as offline now");

    my $offline= &openState($file);
    ok($offline, "TestNormalFunctionality->verifyStateConsistency - managed to open offline circuit");
    my $failureData = $offline->getFailedRequest();
    is(floor($failureData->[0]), floor($time), "TestNormalFunctionality->verifyStateConsistency - verified that failure details were correctly saved");
    is($failureData->[1], 'Failure to restore request from disk', "TestNormalFunctionality->verifyStateConsistency - verified that failure details were correctly saved");
}

# Test consists of creating 3 different established circuits on disk (Expired, Not expired, No lifetime given)
# The circuit manager should take appropiate actions
sub testVSCHandleEstablishedCircuits {

    ### Setup circuit manager
    my ($circuitManager, $session) = setupResourceManager(0.5, 'handle-established-circuits.log', HOUR);
    $circuitManager->Logmsg('Testing event verifyStateConsistency / handling newly found requests on disk');

    my $time = &mytimeofday();

    my $establishedNotYetExpired = createEstablishedCircuit($time - 0.3, '192.168.0.1', '192.168.0.2', undef, $time - 0.3, 'Dummy', 'T2_ANSE_CERN_2', 'T2_ANSE_CERN_Dev', 0.5);
    $establishedNotYetExpired->{STATE_DIR} = "$baseLocation".'/data/circuits';
    $establishedNotYetExpired->saveState();

    my $establishedNotExpired = createEstablishedCircuit($time - 0.3, '192.168.0.1', '192.168.0.2', undef, $time - 0.3, 'Dummy', 'T2_ANSE_CERN_1', 'T2_ANSE_CERN_2', 1.0);
    $establishedNotExpired->{STATE_DIR} = "$baseLocation".'/data/circuits';
    $establishedNotExpired->saveState();

    my $establishedExpired = createEstablishedCircuit($time - 0.6, '192.168.0.1', '192.168.0.2', undef, $time - 0.6, 'Dummy', 'T2_ANSE_CERN_2', 'T2_ANSE_CERN_1', 0.4);
    $establishedExpired->{STATE_DIR} = "$baseLocation".'/data/circuits';
    $establishedExpired->saveState();

    my $establishedNoExpiration = createEstablishedCircuit($time, '192.168.0.1', '192.168.0.2', undef, $time, 'Dummy', 'T2_ANSE_CERN_1', 'T2_ANSE_CERN_Dev');
    $establishedNoExpiration->{STATE_DIR} = "$baseLocation".'/data/circuits';
    $establishedNoExpiration->saveState();

    ### Run POE
    POE::Kernel->run();

    ok(!$circuitManager->{CIRCUITS}{$establishedNotYetExpired->{NAME}}, "TestNormalFunctionality->verifyStateConsistency - we used an established circuit for a bit, then we tore it down");
    ok($circuitManager->{CIRCUITS_HISTORY}{$establishedNotYetExpired->{NAME}}, "TestNormalFunctionality->verifyStateConsistency - torn down used circuit now found in history");
    ok($circuitManager->{CIRCUITS}{$establishedNotExpired->{NAME}}, "TestNormalFunctionality->verifyStateConsistency - used established circuit which doesn't have an expiration date");
    ok(!$circuitManager->{CIRCUITS}{$establishedExpired->{NAME}}, "TestNormalFunctionality->verifyStateConsistency - established circuit which expired is not used");
    ok($circuitManager->{CIRCUITS}{$establishedNoExpiration->{NAME}}, "TestNormalFunctionality->verifyStateConsistency - used established circuit which doesn't have an expiration date");
}

# Test consists of creating three offline circuits. One of them is older than HISTORY_DURATION
# while the other two are not. The circuit manager should restore the two newer ones
sub testVSCOfflineCircuits {

    ### Setup circuit manager
    my ($circuitManager, $session) = setupResourceManager(0.1, 'handle-offline-circuits.log', HOUR);
    $circuitManager->Logmsg('Testing event verifyStateConsistency / handling offline circuits from disk');

    my $time = &mytimeofday();

    my $offlineOld = createOfflineCircuit($time - 20);
    $offlineOld->{STATE_DIR} = "$baseLocation".'/data/circuits';
    $offlineOld->{NODE_A} = 'T2_ANSE_CERN_1';
    $offlineOld->{NODE_B} = 'T2_ANSE_CERN_2';
    $offlineOld->saveState();

    my $offlineNew1 = createOfflineCircuit($time - 10);
    $offlineNew1->{STATE_DIR} = "$baseLocation".'/data/circuits';
    $offlineNew1->{NODE_A} = 'T2_ANSE_CERN_1';
    $offlineNew1->{NODE_B} = 'T2_ANSE_CERN_2';
    $offlineNew1->saveState();

    my $offlineNew2 = createOfflineCircuit();
    $offlineNew2->{STATE_DIR} = "$baseLocation".'/data/circuits';
    $offlineNew2->{NODE_A} = 'T2_ANSE_CERN_1';
    $offlineNew2->{NODE_B} = 'T2_ANSE_CERN_2';
    $offlineNew2->saveState();

    ### Run POE
    POE::Kernel->run();

    my $linkName = $offlineNew1->{NAME};
    ok($circuitManager->{CIRCUITS_HISTORY}{$linkName}, "TestNormalFunctionality->verifyStateConsistency - Restored offline circuits");
    is(keys %{$circuitManager->{CIRCUITS_HISTORY}{$linkName}}, 3,"TestNormalFunctionality->verifyStateConsistency - Restored 2 offline circuits");
    is_deeply($circuitManager->{CIRCUITS_HISTORY}{$linkName}{$offlineNew1->{ID}}, $offlineNew1, "TestNormalFunctionality->verifyStateConsistency - Restored correct offline circuit");
    is_deeply($circuitManager->{CIRCUITS_HISTORY}{$linkName}{$offlineNew2->{ID}}, $offlineNew2, "TestNormalFunctionality->verifyStateConsistency - Restored correct offline circuit");
}

# This is just to have everything in one place for tests of one event
sub testVerifyStateConsistency {
    testVSCMalformedCircuits();
    testVSCMisplacedCircuits();
    testVSCUnclaimedCircuits();
    testVSCSkipIdenticalCircuits();
    testVSCRemoveSimilarCircuits();
    testVSCHandleCircuitRequest();
    testVSCHandleEstablishedCircuits();
    testVSCOfflineCircuits();
}

# Test consists of putting two links on blacklist at different times...
# The circuit manager should only remove one of them from the blacklist
sub testHandleTimer {

    sub iTestTrimBlacklist {
        my $circuitManager = $_[ARG0];

        my $time = &mytimeofday();

        my $circuit1 = createEstablishedCircuit($time, '192.168.0.1', '192.168.0.2', undef, $time, 'WDummy', 'T2_ANSE_CERN_1', 'T2_ANSE_CERN_2');
        my $circuit2 = createEstablishedCircuit($time, '192.168.0.1', '192.168.0.2', undef, $time, 'WDummy', 'T2_ANSE_CERN_2', 'T2_ANSE_CERN_3');

        $circuitManager->addLinkToBlacklist($circuit1, 'Reason 1', 0.1);
        $circuitManager->addLinkToBlacklist($circuit2, 'Reason 2', 1);
    }

    # Create a new circuit manager and setup session
    my ($circuitManager, $session) = setupResourceManager(0.3, 'handleTimer.log', undef,
                                                        [[\&iTestTrimBlacklist, 0.1]]);
    $circuitManager->Logmsg('Testing event handleTimer');

    ### Run POE
    POE::Kernel->run();


    ok(!$circuitManager->{LINKS_BLACKLISTED}{"T2_ANSE_CERN_1-to-T2_ANSE_CERN_2"}, 'TestNormalFunctionality->testHandleTimer: first link was unblacklisted ');
    ok($circuitManager->{LINKS_BLACKLISTED}{"T2_ANSE_CERN_2-to-T2_ANSE_CERN_3"}, 'TestNormalFunctionality->testHandleTimer: second link didn\'t get to be unblacklisted ');
}

# Test consists of calling requestCircuit several times with different invalid parameters
# The circuit manager should not take any of those requests into consideration
sub testRCInvalidCircuitRequests {
    my ($circuitManager, $session) = setupResourceManager(0.1, 'invalid-circuit-requests.log');
    $circuitManager->Logmsg('Testing event requestCircuit');
    my $time = &mytimeofday();

    ### Prepare things to test
    $circuitManager->{CIRCUITS}{'T2_ANSE_CERN_2-to-T2_ANSE_CERN_1'} = createRequestingCircuit($time, 'Dummy', 'T2_ANSE_CERN_2', 'T2_ANSE_CERN_1');;
    POE::Kernel->post($session, 'requestCircuit',  undef, 'T2_ANSE_CERN_1', undef);
    POE::Kernel->post($session, 'requestCircuit',  'T2_ANSE_CERN_2', undef, undef);
    POE::Kernel->post($session, 'requestCircuit',  'T2_ANSE_CERN_1', 'T2_ANSE_CERN_3', undef);

    ### Run POE
    POE::Kernel->run();

    my $nodesUndef = logChecking('invalid-circuit-requests.log', 'Request circuit: Provided link is invalid - will not attempt a circuit request');
    my $outdatedBackend = logChecking('invalid-circuit-requests.log', 'Provided link does not support circuits');
    my $alreadyExists = logChecking('invalid-circuit-requests.log', 'Skipping request for T2_ANSE_CERN_2-to-T2_ANSE_CERN_1 since there is already a request/circuit ongoing');

    ok($nodesUndef, 'TestNormalFunctionality->requestCircuit: Checked log - did not attempt a request with undef nodes');
    ok($outdatedBackend, 'TestNormalFunctionality->requestCircuit: Checked log - did not attempt a request with outdated infos on backend');
    ok($alreadyExists, 'TestNormalFunctionality->requestCircuit: Checked log - did not attempt a request since there\'s already one ongoing');
}

# Test consists of calling requestCircuit two times with valid parameters (one of which will declare a circuit with a limited life)
# This basically tests events requestCircuit, handleRequestResponse and teardownCircuit.
# It should run for 0.7 seconds...
#  @ 0.2 sec: iTestCreationOfRequests   checks that requests are on disk
#  @ 0.4 sec: iTestSwitchToEstablished  checks that requests have been transformed into established circuits, and are on disk
#  @ 0.6 sec: iTestSwitchToOffline      checks that one of the circuits went offline
sub testRCCreatesRequests {

    our $time = &mytimeofday();
    our ($partialIDc1, $partialIDc2);

    # Intermediate test that checks that requests were actually created
    sub iTestCreationOfRequests {
        my $circuitManager = $_[ARG0];

        my $c1 = $circuitManager->{CIRCUITS}{'T2_ANSE_CERN_1-to-T2_ANSE_CERN_2'};
        my $c2 = $circuitManager->{CIRCUITS}{'T2_ANSE_CERN_2-to-T2_ANSE_CERN_1'};
        $partialIDc1 = substr($c1->{ID}, 1, 8);
        $partialIDc2 = substr($c2->{ID}, 1, 8);

        my $fileReq1 = $baseLocation."/data/circuits/requested/T2_ANSE_CERN_1-to-T2_ANSE_CERN_2-$partialIDc1-".formattedTime($time);
        my $fileReq2 = $baseLocation."/data/circuits/requested/T2_ANSE_CERN_2-to-T2_ANSE_CERN_1-$partialIDc2-".formattedTime($time);

        ok(-e $fileReq1, "TestNormalFunctionality->requestCircuit - circuit 1 has been requested");
        ok(-e $fileReq2, "TestNormalFunctionality->requestCircuit - circuit 2 has been requested");

        my ($circuit1, $code1) = &openState($fileReq1);
        my ($circuit2, $code2) = &openState($fileReq2);

        ok($circuit1, "TestNormalFunctionality->requestCircuit - was able to open saved state for circuit1");
        ok($circuit2, "TestNormalFunctionality->requestCircuit - was able to open saved state for circuit2");

        is($circuit1->{NODE_A}, 'T2_ANSE_CERN_1', "TestNormalFunctionality->requestCircuit - circuit 1 from node ok");
        is($circuit1->{NODE_B}, 'T2_ANSE_CERN_2', "TestNormalFunctionality->requestCircuit - circuit 1 to node ok");
        is($circuit2->{NODE_A}, 'T2_ANSE_CERN_2', "TestNormalFunctionality->requestCircuit - circuit 2 from node ok");
        is($circuit2->{NODE_B}, 'T2_ANSE_CERN_1', "TestNormalFunctionality->requestCircuit - circuit 2 to node ok");

        ok($circuitManager->{CIRCUITS}{$circuit1->{NAME}}, "TestNormalFunctionality->requestCircuit - circuit 1 exists in the circuit manager");
        ok($circuitManager->{CIRCUITS}{$circuit2->{NAME}}, "TestNormalFunctionality->requestCircuit - circuit 2 exists in the circuit manager");

        is($circuitManager->{CIRCUITS}{$circuit1->{NAME}}{STATUS}, STATUS_CIRCUIT_REQUESTING, "TestNormalFunctionality->requestCircuit - circuit 1 status in circuit manager is correct");
        is($circuitManager->{CIRCUITS}{$circuit2->{NAME}}{STATUS}, STATUS_CIRCUIT_REQUESTING, "TestNormalFunctionality->requestCircuit - circuit 2 status in circuit manager is correct");
    }

    # Intermediate test that checks that requests were switched to active circuits
    sub iTestSwitchToEstablished {
        my $circuitManager = $_[ARG0];

        my $fileReq1 = $baseLocation."/data/circuits/requested/T2_ANSE_CERN_1-to-T2_ANSE_CERN_2-$partialIDc1-".formattedTime($time);
        my $fileReq2 = $baseLocation."/data/circuits/requested/T2_ANSE_CERN_2-to-T2_ANSE_CERN_1-$partialIDc2-".formattedTime($time);
        my $fileEst1 = $baseLocation."/data/circuits/online/T2_ANSE_CERN_1-to-T2_ANSE_CERN_2-$partialIDc1-".formattedTime($time+0.3);
        my $fileEst2 = $baseLocation."/data/circuits/online/T2_ANSE_CERN_2-to-T2_ANSE_CERN_1-$partialIDc2-".formattedTime($time+0.3);

        ok(!-e $fileReq1, "TestNormalFunctionality->requestCircuit - request for circuit 1 has been removed");
        ok(!-e $fileReq2, "TestNormalFunctionality->requestCircuit - request for circuit 2 has been removed");
        ok(-e $fileEst1, "TestNormalFunctionality->requestCircuit - circuit 1 has been established");
        ok(-e $fileEst2, "TestNormalFunctionality->requestCircuit - circuit 2 has been established");

        my ($circuit1, $code1) = &openState($fileEst1);
        my ($circuit2, $code2) = &openState($fileEst2);

        is($circuit1->{IP_A}, '188.184.134.192', "TestNormalFunctionality->requestCircuit - circuit 1 from ip ok");
        is($circuit1->{IP_B}, '128.142.135.112', "TestNormalFunctionality->requestCircuit - circuit 1 to ip ok");
        ok(!$circuit1->{LIFETIME}, "TestNormalFunctionality->requestCircuit - circuit 1 doesn't have a life ... set");
        is($circuit2->{IP_A}, '128.142.135.112', "TestNormalFunctionality->requestCircuit - circuit 2 from ip ok");
        is($circuit2->{IP_B}, '188.184.134.192', "TestNormalFunctionality->requestCircuit - circuit 2 to ip ok");
        ok($circuit2->{LIFETIME}, "TestNormalFunctionality->requestCircuit - circuit 2 has a life set");

        is($circuitManager->{CIRCUITS}{$circuit1->{NAME}}{STATUS}, STATUS_CIRCUIT_ONLINE, "TestNormalFunctionality->requestCircuit - circuit 1 status in circuit manager is correct");
        is($circuitManager->{CIRCUITS}{$circuit2->{NAME}}{STATUS}, STATUS_CIRCUIT_ONLINE, "TestNormalFunctionality->requestCircuit - circuit 2 status in circuit manager is correct");
    }

    # Intermediate test that checks that the circuit which had a lifetime expired
    sub iTestSwitchToOffline {
        my $circuitManager = $_[ARG0];

        my $fileEst1 = $baseLocation."/data/circuits/online/T2_ANSE_CERN_1-to-T2_ANSE_CERN_2-$partialIDc1-".formattedTime($time+0.3);
        my $fileEst2 = $baseLocation."/data/circuits/online/T2_ANSE_CERN_2-to-T2_ANSE_CERN_1-$partialIDc2-".formattedTime($time+0.3);
        my $fileOff2 = $baseLocation."/data/circuits/offline/T2_ANSE_CERN_2-to-T2_ANSE_CERN_1-$partialIDc2-".formattedTime($time+0.5);

        ok(-e $fileEst1, "TestNormalFunctionality->requestCircuit - circuit 1 hasn't expired");
        ok(!-e $fileEst2, "TestNormalFunctionality->requestCircuit - circuit 2 has expired");
        ok(-e $fileOff2, "TestNormalFunctionality->requestCircuit - circuit 2 has been declared as offline");

        my ($circuit2, $code2) = &openState($fileOff2);
        is($circuit2->{STATUS}, STATUS_CIRCUIT_OFFLINE, "TestNormalFunctionality->requestCircuit - circuit 2 is indeed offline");

        ok(!$circuitManager->{CIRCUITS}{$circuit2->{NAME}}, "TestNormalFunctionality->requestCircuit - circuit 2 exists in the circuit manager's history");
        is($circuitManager->{CIRCUITS}{'T2_ANSE_CERN_1-to-T2_ANSE_CERN_2'}{STATUS}, STATUS_CIRCUIT_ONLINE, "TestNormalFunctionality->requestCircuit - circuit 1 status in circuit manager is correct");
        is($circuitManager->{CIRCUITS_HISTORY}{$circuit2->{NAME}}{$circuit2->{ID}}{STATUS}, STATUS_CIRCUIT_OFFLINE, "TestNormalFunctionality->requestCircuit - circuit 2 status in circuit manager is correct");
    }

    my ($circuitManager, $session) = setupResourceManager(0.7, 'creating-circuit-requests.log', undef,
                                                            [[\&iTestCreationOfRequests, 0.2],
                                                             [\&iTestSwitchToEstablished, 0.4],
                                                             [\&iTestSwitchToOffline, 0.6]]);
    $circuitManager->Logmsg('Testing events requestCircuit, handleRequestResponse and teardownCircuit');
    $circuitManager->{BACKEND}{TIME_SIMULATION} = 0.3; # Wait 300ms before producing event

    POE::Kernel->post($session, 'requestCircuit',  'T2_ANSE_CERN_1', 'T2_ANSE_CERN_2');
    POE::Kernel->post($session, 'requestCircuit',  'T2_ANSE_CERN_2', 'T2_ANSE_CERN_1', 0.2);

    ### Run POE
    POE::Kernel->run();
}

# Test consists of creating a circuit request which we then leave to expire
# The circuit manager should place it in the offline folder and flag it as blacklisted
sub testRCExpiringCircuitRequests {
    my $time = &mytimeofday();

    my ($circuitManager, $session) = setupResourceManager(0.3, 'circuit-request-expires.log');
    $circuitManager->Logmsg('Testing event requestCircuit');
    $circuitManager->{BACKEND}{TIME_SIMULATION} = 0.2;
    $circuitManager->{CIRCUIT_REQUEST_TIMEOUT} = 0.1;

    POE::Kernel->post($session, 'requestCircuit', 'T2_ANSE_CERN_1', 'T2_ANSE_CERN_2');

    ### Run POE
    POE::Kernel->run();

    my @offKeys = keys %{$circuitManager->{CIRCUITS_HISTORY}{'T2_ANSE_CERN_1-to-T2_ANSE_CERN_2'}};

    my $circuitID = substr($offKeys[0], 1, 8);
    my $fileOff = $baseLocation."/data/circuits/offline/T2_ANSE_CERN_1-to-T2_ANSE_CERN_2-$circuitID-".formattedTime($time + 0.1);
    ok(-e $fileOff, "TestNormalFunctionality->requestCircuit - circuit 1 request has expired");
    ok(!$circuitManager->{CIRCUITS}{'T2_ANSE_CERN_1-to-T2_ANSE_CERN_2'}, "TestNormalFunctionality->requestCircuit - circuit 1 request has expired and was removed from CIRCUITS");
    ok($circuitManager->{CIRCUITS_HISTORY}{'T2_ANSE_CERN_1-to-T2_ANSE_CERN_2'}, "TestNormalFunctionality->requestCircuit - circuit 1 request has expired and was placed into CIRCUITS_HISTORY");
    ok($circuitManager->{LINKS_BLACKLISTED}{'T2_ANSE_CERN_1-to-T2_ANSE_CERN_2'}, "TestNormalFunctionality->requestCircuit - circuit 1 request has expired and was placed into LINKS_BLACKLISTED");
}

# Tests how the requestCircuit events reacts to different parameters passed to it
# The circuit manager should
# - ignore undef nodes
# - ignore requests if the backend doesn't support nodes
# - actually request a circuit
#   - create circuit
#   - save state
#   - start watchdog for timeouts
#   - correctly handle those timeouts
# - update internal data for successful circuit requests
sub testRequestCircuit {
    testRCInvalidCircuitRequests();
    testRCCreatesRequests();
    testRCExpiringCircuitRequests();
}

# Test consists of establishing two circuits then simulating transfer failures on both of them
# The circuit manager should only flag the one which exceeds MAX_HOURLY_FAILURE_RATE
# 100 failures / hours
sub testTransferFailure {

    our ($partialIDc1, $partialIDc2);

    # Simulate failures
    sub iFailTransfers {
        my $circuitManager = $_[ARG0];

        my $circuit1 = $circuitManager->{CIRCUITS}{'T2_ANSE_CERN_1-to-T2_ANSE_CERN_2'};
        my $circuit2 = $circuitManager->{CIRCUITS}{'T2_ANSE_CERN_2-to-T2_ANSE_CERN_1'};

        $partialIDc1 = substr($circuit1->{ID}, 1, 8);
        $partialIDc2 = substr($circuit2->{ID}, 1, 8);

        $circuit1->{VERBOSE} = 0;
        $circuit2->{VERBOSE} = 0;

        # Simulate failure of transfers on both links
        for (my $i = 0; $i < 100; $i++) {
            $circuitManager->transferFailed($circuit1, $i);
            $circuitManager->transferFailed($circuit2, $i);
        }


        # Deal the final blow to circuit 2
        $circuitManager->transferFailed($circuit2, 101);
    }

    my $time = &mytimeofday();

    my ($circuitManager, $session) = setupResourceManager(0.4, 'circuit-request-expires.log', undef,
                                                            [[\&iFailTransfers, 0.2]]);
    $circuitManager->Logmsg('Testing event transferFailed');
    $circuitManager->{BACKEND}{TIME_SIMULATION} = 0.1;

    POE::Kernel->post($session, 'requestCircuit',  'T2_ANSE_CERN_1', 'T2_ANSE_CERN_2');
    POE::Kernel->post($session, 'requestCircuit',  'T2_ANSE_CERN_2', 'T2_ANSE_CERN_1');

    POE::Kernel->run();

    my $file1 = $baseLocation."/data/circuits/online/T2_ANSE_CERN_1-to-T2_ANSE_CERN_2-$partialIDc1-".formattedTime($time + 0.1);
    my $file2 = $baseLocation."/data/circuits/offline/T2_ANSE_CERN_2-to-T2_ANSE_CERN_1-$partialIDc2-".formattedTime($time + 0.2);

    ok(-e $file1, "TestNormalFunctionality->testTransferFailure - circuit 1 wasn't blacklisted yet");
    ok(-e $file2, "TestNormalFunctionality->testTransferFailure - circuit 2 was put offline");
    ok($circuitManager->{LINKS_BLACKLISTED}{'T2_ANSE_CERN_2-to-T2_ANSE_CERN_1'}, "TestNormalFunctionality->testTransferFailure - circuit 2 was blacklisted");
}

File::Path::rmtree("$baseLocation".'/logs', 1, 1) if (-d "$baseLocation".'/logs');
File::Path::make_path("$baseLocation".'/logs', { error => \my $err});

testHelperMethods();
testVerifyStateConsistency();
testHandleTimer();
testRequestCircuit();
testTransferFailure();

done_testing();

1;
