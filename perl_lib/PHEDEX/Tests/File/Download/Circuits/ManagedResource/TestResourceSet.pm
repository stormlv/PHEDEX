package PHEDEX::Tests::File::Download::Circuits::ManagedResource::TestResourceSet;

use strict;
use warnings;

use Test::More;

use PHEDEX::File::Download::Circuits::Helpers::Utils::Utils;
use PHEDEX::File::Download::Circuits::ManagedResource::ResourceSet;
use PHEDEX::Tests::Helpers::ObjectCreation;

sub testGenericFunctionality {
    my $msg = "TestResourceSet->testGenericFunctionality";
    my $set = PHEDEX::File::Download::Circuits::ManagedResource::ResourceSet->new(maxResources => 2);
    
    my $testCircuit1 = createNewCircuit();
    my $testCircuit2 = createNewCircuit();
    my $testCircuit3 = createNewCircuit();
    
    ok($set->isEmpty(), "$msg: Set is empty initially");
    
    $set->addResource($testCircuit1);
    $set->addResource($testCircuit2);
    
    is($set->countResources(), 2, "$msg: Two resources successfully added to the set");
    ok($set->resourceExists($testCircuit1), "$msg: Correctly added first resource");
    ok($set->resourceExists($testCircuit2), "$msg: Correctly added seconds resource");
    
    ok(!$set->canAddResource(), "$msg: Cannot add a third resource");
    
    $set->deleteResource($testCircuit2);
    
    ok(!$set->resourceExists($testCircuit2), "$msg: Correctly deleted the second resource");
    ok($set->canAddResource(), "$msg: Can add a new resource");
    
    $set->addResource($testCircuit3);
    
    is($set->countResources(), 2, "$msg: Two resources successfully added to the set");
    ok($set->resourceExists($testCircuit1), "$msg: Correctly added first resource");
    ok($set->resourceExists($testCircuit3), "$msg: Correctly added third resource");
    
    $testCircuit1->bandwidthAllocated(120);
    $testCircuit3->bandwidthAllocated(100);
    
    is_deeply($set->getResourceByBW(), $testCircuit1, "$msg: Correctly returned resource with the highest allocated bw");
    
    $testCircuit1->scope('Generic');
    $testCircuit3->scope('Analysis');
    
    is_deeply($set->getResourceByScope('Generic'), $testCircuit1, "$msg: Correctly returned resource matching the scope");
    is_deeply($set->getResourceByScope('Analysis'), $testCircuit3, "$msg: Correctly returned resource matching the scope");
}

sub testSerialization {
    my $msg = "TestResourceSet->testSerialization";
    
    my $circuits = 10;
    my $set = PHEDEX::File::Download::Circuits::ManagedResource::ResourceSet->new(maxResources => $circuits, stateDir => '/tmp/vlad/');

    my $resourceHash = {};
    for (my $i = 0; $i < $circuits; $i++) {
        my $testResource = createNewCircuit();
        $resourceHash->{$testResource->id} = $testResource;
        $set->addResource($testResource);
    }

    $set->saveState();
    
    my $openedSet = PHEDEX::File::Download::Circuits::ManagedResource::ResourceSet->load("/tmp/vlad/".$set->id.".set");
    ok($openedSet, "$msg: Successfully opened set");

    foreach my $resourceId (keys %{$resourceHash}) {
        my $openedResource = $openedSet->getResource($resourceId);
        my $originalResource = $set->getResource($resourceId);
        ok($openedResource, "$msg: Resource exists in opened set");
        ok(&compareObject($openedResource, $originalResource), "$msg: Resources match")
    }
    
}

testGenericFunctionality();
testSerialization();

done_testing();

1;
