package PHEDEX::Tests::File::Download::Circuits::ManagedResource::TestBandwidth;

use strict;
use warnings;

use File::Path;
use IO::File;
use Test::More;

use PHEDEX::Core::Timing;

use PHEDEX::File::Download::Circuits::Common::Constants;
use PHEDEX::File::Download::Circuits::ManagedResource::Bandwidth;
use PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource;
use PHEDEX::File::Download::Circuits::ManagedResource::Node;
use PHEDEX::File::Download::Circuits::ManagedResource::Path;

use PHEDEX::Tests::Helpers::ObjectCreation;


# Trivial test consists of creating a circuit and making sure parameters are initialized correctly
sub testInitialisation {
    my $msg = "TestBandwidth->testInitialisation";
    
    my $nodeA = PHEDEX::File::Download::Circuits::ManagedResource::Node->new(siteName => 'NodeA', endpointName => 'STP1', maxBandwidth => 111);
    my $nodeB = PHEDEX::File::Download::Circuits::ManagedResource::Node->new(siteName => 'NodeB', endpointName => 'STP2', maxBandwidth => 222);
    my $path = PHEDEX::File::Download::Circuits::ManagedResource::Path->new(nodeA => $nodeA, nodeB => $nodeB, type => 'Layer2');
    
    # Create circuit and initialise it
    my $testBandwidth = PHEDEX::File::Download::Circuits::ManagedResource::Bandwidth->new(backendType => 'Dummy',
                                                                                          path => $path);
    
    ok($testBandwidth->id, "$msg: ID set");
    is($testBandwidth->path->getSiteNameA, 'NodeA', "$msg: Object initialisation - Node_A set");
    is($testBandwidth->path->getSiteNameB, 'NodeB', "$msg: Object initialisation - Node_B set");
    is($testBandwidth->backendType, 'Dummy', "$msg: Object initialisation - Backend set");
    is($testBandwidth->status, 'Offline', "$msg: Object initialisation - Status set to offline");
    is($testBandwidth->stateDir, '/tmp/managed/Bandwidth', "$msg: Object initialisation - Correct state folder set");
    is($testBandwidth->scope, 'Generic', "$msg: Object initialisation - Scope set");
    is($testBandwidth->resourceType, 'Bandwidth', "$msg: Object initialisation - Correct resource type set");
    is($testBandwidth->bandwidthStep, 1, "$msg: Object initialisation - Bandwidth step set");
    is($testBandwidth->bandwidthMin, 0, "$msg: Object initialisation - Min bandwidth set");
    is($testBandwidth->bandwidthMax, 1000, "$msg: Object initialisation - Max bandwidth set");
    is($testBandwidth->bandwidthAllocated, 0, "$msg: Object initialisation - Allocated bandwidth set");
}

# Testing the getExpirationTime, isExpired and getLinkName subroutines
sub testHelperMethods {
    my $msg = "TestBandwidth->testHelperMethods";
    
    my $bandwidth1 = createOfflineBandwidth();
    my $bandwidth2 = createUpdatingBandwidth();
    my $bandwidth3 = createRunningBandwidth();

    is($bandwidth1->getLinkName(), 'T2_ANSE_GENEVA-T2_ANSE_AMSTERDAM', "$msg: Name was set correctly");

    # Test getSavePaths
    ok($bandwidth1->getSavePaths() =~ /offline/, "$msg: getSavePaths works as it should on an offline bandwidth");
    ok($bandwidth2->getSavePaths() =~ /offline/, "$msg: getSavePaths works as it should on an updating bandwidth");
    ok($bandwidth3->getSavePaths() =~ /online/, "$msg: getSavePaths works as it should on a running bandwidth");
    
    # Test validateBandwidth
    is($bandwidth3->validateBandwidth(501.2), ERROR_GENERIC, "$msg: validateBandwidth rejects value not a multiple of step size");
    is($bandwidth3->validateBandwidth(1001), ERROR_GENERIC, "$msg: validateBandwidth rejects value over max");
    is($bandwidth3->validateBandwidth(-1), ERROR_GENERIC, "$msg: validateBandwidth rejects value under min");
}

# Testing changing of status of the bandwidth object
sub testStatusChange {
    my $msg = "TestBandwidth->testStatusChange";
    
    my ($offlineBW, $runningBW, $updatingBW);
    
    # Checking registerUpdateRequest
    $offlineBW = createOfflineBandwidth();
    $runningBW = createRunningBandwidth();
    $updatingBW = createUpdatingBandwidth();
   
    is($updatingBW->registerUpdateRequest(1000), ERROR_GENERIC, "$msg: failed to register update on updating bw");
    is($offlineBW->registerUpdateRequest(1000), OK, "$msg: registered update request on an offline bw");
    is($runningBW->registerUpdateRequest(1000), OK, "$msg: registered update request on an online bw");
    
    is($offlineBW->status, 'Pending', "$msg: updating object status is ok");
    is($offlineBW->bandwidthAllocated, 0, "$msg: updating object allocated bw is 0");
    is($offlineBW->bandwidthRequested, 1000, "$msg: updating object requested bw is 500");
    
    is($runningBW->status, 'Pending', "$msg: updating object status is ok");
    is($runningBW->bandwidthAllocated, 500, "$msg: updating object allocated bw is 0");
    is($runningBW->bandwidthRequested, 1000, "$msg: updating object requested bw is undef");
    
    # Checking registerUpdateFailed
    $offlineBW = createOfflineBandwidth();
    $runningBW = createRunningBandwidth();
    $updatingBW = createUpdatingBandwidth();
    
    is($offlineBW->registerUpdateFailed(), ERROR_GENERIC, "$msg: cannot register a failed update on an offline bw");
    is($runningBW->registerUpdateFailed(), ERROR_GENERIC, "$msg: cannot register a failed update on an online bw");
    
    is($updatingBW->status, 'Pending', "$msg: updating object status is ok");
    is($updatingBW->bandwidthAllocated, 0, "$msg: updating object allocated bw is 0");
    is($updatingBW->bandwidthRequested, 500, "$msg: updating object requested bw is 500");
    
    is($updatingBW->registerUpdateFailed(), OK, "$msg: registered update failure on updating bw");
    is($updatingBW->status, 'Offline', "$msg: updating object status is ok");
    is($updatingBW->bandwidthAllocated, 0, "$msg: updating object allocated bw is 0");
    is($updatingBW->bandwidthRequested, 500, "$msg: updating object requested bw is undef");

    # Checking registerUpdateSuccessful
    $offlineBW = createOfflineBandwidth();
    $runningBW = createRunningBandwidth();
    $updatingBW = createUpdatingBandwidth();

    is($offlineBW->registerUpdateSuccessful(), ERROR_GENERIC, "$msg: cannot register a successful update on an offline bw");
    is($runningBW->registerUpdateSuccessful(), ERROR_GENERIC, "$msg: cannot register a successful update on an online bw");
    
    is($updatingBW->status, 'Pending', "$msg: updating object status is ok");
    is($updatingBW->bandwidthAllocated, 0, "$msg: updating object allocated bw is 0");
    is($updatingBW->bandwidthRequested, 500, "$msg: updating object requested bw is 500");
    
    is($updatingBW->registerUpdateSuccessful(), OK, "$msg: registered update success on updating bw");
    is($updatingBW->status, 'Online', "$msg: updating object status is ok");
    is($updatingBW->bandwidthAllocated, 500, "$msg: updating object allocated bw is 500");
    is($updatingBW->bandwidthRequested, 500, "$msg: updating object requested bw is undef");
}

# TODO: Test save/open/remove (although circuit tests show that it's fine...)
testInitialisation();
testHelperMethods();
testStatusChange();

done_testing();

1;
