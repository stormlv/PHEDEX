package PHEDEX::Tests::Helpers::ObjectCreation;

use strict;
use warnings;

use PHEDEX::Core::Timing;

use PHEDEX::File::Download::Circuits::Common::Constants;
use PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource;

use base 'Exporter';

our @EXPORT = qw(createNewCircuit createRequestingCircuit createEstablishedCircuit createTask);

sub createNewCircuit {
    my ($req_time, $backend, $from, $to, $life, $req_bandwidth) = @_;

    $backend = $backend || 'Other::Dummy';
    $from = $from || 'T2_ANSE_GENEVA';
    $to = $to || 'T2_ANSE_AMSTERDAM';
    $req_time = $req_time || 1433116800;

    my $nodeA = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => $from, netName => 'STP1', maxBandwidth => 111);
    my $nodeB = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => $to,   netName => 'STP2', maxBandwidth => 222);
    my $path = PHEDEX::File::Download::Circuits::ManagedResource::Core::Path->new(nodeA => $nodeA, nodeB => $nodeB, type => 'Layer2');

    return PHEDEX::File::Download::Circuits::ManagedResource::NetworkResource->new(backendName => $backend, path => $path);
}

sub createRequestingCircuit {
    my ($req_time, $backend, $from, $to, $life, $req_bandwidth) = @_;

    $from = $from || 'T2_ANSE_GENEVA';
    $to = $to || 'T2_ANSE_AMSTERDAM';
    $req_time = $req_time || 1398426904;
    $backend = $backend || 'Other::Dummy';
    
    my $nodeA = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => $from, netName => 'STP1', maxBandwidth => 111);
    my $nodeB = PHEDEX::File::Download::Circuits::ManagedResource::Core::Node->new(appName => $to, netName => 'STP2', maxBandwidth => 222);
    my $path = PHEDEX::File::Download::Circuits::ManagedResource::Core::Path->new(nodeA => $nodeA, nodeB => $nodeB, type => 'Layer2');
    
    my $testCircuit = PHEDEX::File::Download::Circuits::ManagedResource::Circuit->new(backendName => $backend,
                                                                                      path => $path);

    $testCircuit->registerRequest($life, $req_bandwidth);
    $testCircuit->requestedTime($req_time);

    return $testCircuit;
}

sub createEstablishedCircuit {
    my ($est_time, $from_ip, $to_ip, $al_bandwidth, $req_time, $backend, $from, $to, $life, $req_bandwidth) = @_;

    $from_ip = $from_ip || '192.168.0.1';
    $to_ip = $to_ip || '192.168.0.2';

    $est_time = $est_time || 1398426910;

    my $testCircuit = createRequestingCircuit($req_time, $backend, $from, $to, $life, $req_bandwidth);
    $testCircuit->registerEstablished($from_ip, $to_ip, $al_bandwidth);
    $testCircuit->establishedTime($est_time);

    return $testCircuit;
}

sub createTask {
    my ($startTime, $size, $jobsize, $jobDuration) = @_;

    return {
        TASKID          =>  int(rand(100000000)),
        JOBID           =>  "job.$startTime",
        JOBSIZE         =>  $jobsize,
        PRIORITY        =>  1,

        FILESIZE        =>  $size,
        STARTED         =>  $startTime,
        FINISHED        =>  $startTime + $jobDuration * MINUTE,

        FROM_PFN        =>  'fdt://192.168.0.1:8444/store/data/tag1.root',
        TO_PFN          =>  'fdt://192.168.0.2:8444/store/data/tag2.root',

        XFER_CODE       =>  1,
        REPORT_CODE     =>  1,
    }
}

1;