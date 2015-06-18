package PHEDEX::Tests::File::Download::Circuits::ManagedResource::TestNetworkResource;

use strict;
use warnings;

use IO::File;
use Test::More;

use PHEDEX::File::Download::Circuits::Common::Constants;
use PHEDEX::File::Download::Circuits::Common::Failure;
use PHEDEX::File::Download::Circuits::Helpers::Utils::Utils;
use PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource;
use PHEDEX::File::Download::Circuits::ManagedResource::Core::Node;
use PHEDEX::File::Download::Circuits::ManagedResource::Core::Path;
use PHEDEX::File::Download::Circuits::ResourceManager::ResourceManagerConstants;
use PHEDEX::Tests::Helpers::ObjectCreation;

# Self explaining test
sub testInitialisation {
    my $msg = "TestNetworkResource->testInitialisation";
    
    # Create objects required for resource construction
    my $nodeA = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => 'NodeA', netName => 'STP1', maxBandwidth => 111);
    my $nodeB = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => 'NodeB', netName => 'STP2', maxBandwidth => 222);
    my $path = PHEDEX::File::Download::Circuits::ManagedResource::Core::Path->new(nodeA => $nodeA, nodeB => $nodeB, type => 'Layer2');
    
    my $resource = PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource->new(backendName => 'Dummy', path => $path);
    
    # Provide all the correct parameters to the initialisation and test to see if they were all set in the object
    is($resource->backendName,          "Dummy",        "$msg: Initialisation ok (backend matches)");
    is($resource->bandwidthAllocated,   0,              "$msg: Initialisation ok (bandwidthAllocated matches)");
    is($resource->bandwidthRequested,   0,              "$msg: Initialisation ok (bandwidthRequested matches)");
    is($resource->bandwidthUsed,        0,              "$msg: Initialisation ok (bandwidthUsed matches)");
    ok($resource->id,                                   "$msg: Initialisation ok (id was set)");
    ok($resource->lastStatusChange,                     "$msg: Initialisation ok (remembered last status change)");
    is($resource->lifetime,             6*3600,         "$msg: Initialisation ok (lifetime matches)");
    ok($resource->path,                                 "$msg: Initialisation ok (path was set)");
    is($resource->scope,                "Generic",      "$msg: Initialisation ok (scope matches)");
    is($resource->stateDir,             "/tmp/resources","$msg: Initialisation ok (status matches)");
    is($resource->status,               "Created",      "$msg: Initialisation ok (status matches)");
}
 
sub testHelperMethods {
    my $msg = "TestNetworkResource->testHelperMethods";
    
    ### Test getExpirationTime ###
    my $createdResource = createNewCircuit(); 
    ok(!$createdResource->getExpirationTime, "$msg (getExpirationTime): Cannot get expiration time (resource is offline)");
    $createdResource->status('Online');
    ok(!$createdResource->getExpirationTime, "$msg (getExpirationTime): Cannot get expiration time (resource is online but the establishedTime time is not set)");
    my $timeNow = &mytimeofday();
    # Set established time 5h in the past
    my $timeEstablished = $timeNow - 5*3600; 
    $createdResource->establishedTime($timeEstablished);
    # Test that the expiration time is 1h in the future
    is($createdResource->getExpirationTime, $timeNow + 3600, "$msg (getExpirationTime): Expiration time correctly retrieved");
     
    ### Test isExpired ###
    # Check that the circuit hasn't expired yet
    ok(! $createdResource->isExpired(), "$msg (isExpired): Circuit hasn't expired yet");
    # Set established time back 59min 59 seconds
    $createdResource->establishedTime($createdResource->establishedTime - 3599);
    # Circuit shouldn't have expired
    ok(! $createdResource->isExpired(), "$msg (isExpired): Circuit hasn't expired yet");
    # Set established time back one more second 
    $createdResource->establishedTime($createdResource->establishedTime - 1);
    ok($createdResource->isExpired(), "$msg (isExpired): Circuit expired");
    
    ### Test getSaveLocation ###
    is($createdResource->getSaveLocation, "/tmp/resources/Online", "$msg (getSaveLocation): Save location correctly set");
    $createdResource->status("Created");
    is($createdResource->getSaveLocation, "/tmp/resources/Created", "$msg (getSaveLocation): Save location correctly set");
    
    ### Test getSaveFilename ###
    $createdResource->lastStatusChange(1433116800);
    my $fileName = "T2_ANSE_AMSTERDAM-T2_ANSE_GENEVA-".substr($createdResource->id, 1, 7)."-20150601-00h00m00";
    is($createdResource->getSaveFilename, $fileName, "$msg (getSaveFilename): File name correctly set");
    
    ### Test getFullSavePath ###
    is($createdResource->getFullSavePath, "/tmp/resources/Created/T2_ANSE_AMSTERDAM-T2_ANSE_GENEVA-".substr($createdResource->id, 1, 7)."-20150601-00h00m00", "$msg (getFullSavePath): Full path correctly set");
}

sub testSerialisation {
    my $msg = "TestNetworkResource->testSerialisation";
    
    ### Test saveState ###
    # Save
    my $createdResource = createNewCircuit();
    $createdResource->stateDir("/tmp/vlad/resources");
    $createdResource->{id} = "4529c1df-fd70-4e52-92f9-c55111b54fcb";
    $createdResource->{lastStatusChange} = 1433116800;
    $createdResource->saveState();
    
    # Check that the file has been saved
    my $location = $createdResource->getSaveLocation();
    my $fullPath = $createdResource->getFullSavePath(); 
    ok(-d $location, "$msg (saveState): Folder exists");
    ok(-e $fullPath, "$msg (saveState): Full path exists");
    
    # Attempt to open object
    # We also need to check that a Moose object has indeed been constructed, instead of just a simple hash
    my $openedResource = PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource->load("/tmp/vlad/resources/Created/T2_ANSE_AMSTERDAM-T2_ANSE_GENEVA-529c1df-20150601-00h00m00");
    is($openedResource->backendName, "Other::Dummy", "$msg (saveState): Checking serialization");
    is($openedResource->bandwidthAllocated, 0, "$msg (saveState): Checking serialization");
    is($openedResource->bandwidthRequested, 0, "$msg (saveState): Checking serialization");
    is($openedResource->bandwidthUsed, 0, "$msg (saveState): Checking serialization");
    is($openedResource->id, "4529c1df-fd70-4e52-92f9-c55111b54fcb", "$msg (saveState): Checking serialization");
    is($openedResource->lastStatusChange, 1433116800, "$msg (saveState): Checking serialization");
    is($openedResource->lifetime, 21600, "$msg (saveState): Checking serialization");
    is($openedResource->path->bidirectional, 1, "$msg (saveState): Checking serialization");
    is($openedResource->path->maxBandwidth, 111, "$msg (saveState): Checking serialization");
    is($openedResource->path->maxCircuits, 10, "$msg (saveState): Checking serialization");
    is($openedResource->path->type, "Layer2", "$msg (saveState): Checking serialization");
    is($openedResource->path->nodeA->appName, "T2_ANSE_GENEVA", "$msg (saveState): Checking serialization");
    is($openedResource->path->nodeA->maxBandwidth, 111, "$msg (saveState): Checking serialization");
    is($openedResource->path->nodeA->netName, "STP1", "$msg (saveState): Checking serialization");
    is($openedResource->path->nodeB->appName, "T2_ANSE_AMSTERDAM", "$msg (saveState): Checking serialization");
    is($openedResource->path->nodeB->maxBandwidth, 222, "$msg (saveState): Checking serialization");
    is($openedResource->path->nodeB->netName, "STP2", "$msg (saveState): Checking serialization");
    is($openedResource->scope, "Generic", "$msg (saveState): Checking serialization");
    is($openedResource->stateDir, "/tmp/vlad/resources", "$msg (saveState): Checking serialization");
    is($openedResource->status, "Created", "$msg (saveState): Checking serialization");
    is($openedResource->verbose, 1, "$msg (saveState): Checking serialization");
    
    # Remove the object from folder
    $openedResource->removeState();
    ok(-d $location, "$msg (removeState): Folder exists");
    ok(! -e $fullPath, "$msg (removeState): Full path exists");
}

sub testSettingStatus {
    my $msg = "TestNetworkResource->testSettingStatus";
    my $resource = createNewCircuit();

    # Check change from created
    is($resource->status, "Created", "$msg: Initial state ok");
    ok(!$resource->setStatus("Updating"), "$msg: Cannot change state from Created to Updating");
    ok(!$resource->setStatus("Online"), "$msg: Cannot change state from Created to Online");
    ok(!$resource->setStatus("Offline"), "$msg: Cannot change state from Created to Offline");
    is($resource->setStatus("Requesting", 1000), "Requesting", "$msg: Status Change successful to Requesting from Created");
    
    # Check change from Requesting  
    ok(!$resource->setStatus("Created"), "$msg: Cannot change state from Requesting to Created");
    ok(!$resource->setStatus("Updating"), "$msg: Cannot change state from Requesting to Updating");
    is($resource->setStatus("Online", 1000, "ID1"), "Online", "$msg: Status Change successful to Online from Requesting");
    
    # Check change from Online
    ok(!$resource->setStatus("Created"), "$msg: Cannot change state from Online to Created");
    ok(!$resource->setStatus("Requesting"), "$msg: Cannot change state from Online to Requesting");
    is($resource->setStatus("Updating", 1000), "Updating", "$msg: Status Change successful to Online from Updating");
    
    # Check change from Updating
    ok(!$resource->setStatus("Created"), "$msg: Cannot change state from Updating to Created");
    ok(!$resource->setStatus("Requesting"), "$msg: Cannot change state from Updating to Requesting");
    is($resource->setStatus("Online", 1000, "ID1"), "Online", "$msg: Status Change successful to Updating from Online");
    
    # Finally change to Offline
    is($resource->setStatus("Offline"), "Offline", "$msg: Status Change successful to Offline from Online");
}

sub testFailureHandling {
    my $msg = "TestNetworkResource->testFailureHandling";
    my $resource = createNewCircuit();
    $resource->addFailure(PHEDEX::File::Download::Circuits::Common::Failure->new(comment => "bla", time => 1));
    ok($resource->status('Created'), "$msg: Circuit hasn't gone offline yet due to failures");
    for (my $i = 0; $i < $resource->maxFailureCount; $i ++ ) {
        $resource->addFailure(PHEDEX::File::Download::Circuits::Common::Failure->new(comment => "bla", time => 1));
    }
    ok($resource->status('Offline'), "$msg: Circuit went offline due to too many failures");
}

testInitialisation();
testHelperMethods();
testSerialisation();
testSettingStatus();
testFailureHandling();

done_testing();

1;
