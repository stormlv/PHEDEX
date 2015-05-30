package PHEDEX::Tests::File::Download::Circuits::ManagedResource::TestResourceSet;

use strict;
use warnings;

use Test::More;

use PHEDEX::File::Download::Circuits::ManagedResource::ResourceSet;

use PHEDEX::Tests::Helpers::ObjectCreation;


my $msg = "TestResourceSet";


my $set = PHEDEX::File::Download::Circuits::ManagedResource::ResourceSet->new(maxResources => 2);

my $testCircuit1 = createOfflineCircuit();
my $testCircuit2 = createOfflineCircuit();
my $testCircuit3 = createOfflineCircuit();

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

done_testing();

1;
