package PHEDEX::Tests::File::Download::Helpers::ObjectCreation;

use strict;
use warnings;

use PHEDEX::File::Download::Circuits::Circuit;
use PHEDEX::File::Download::Circuits::Constants;
use PHEDEX::Core::Timing;
use base 'Exporter';

our @EXPORT = qw(createRequestingCircuit createEstablishedCircuit createOfflineCircuit createTask);

sub createRequestingCircuit {
    my ($req_time, $backend, $from, $to, $life, $req_bandwidth) = @_;
    
    $from = $from || 'T2_ANSE_GENEVA';    
    $to = $to || 'T2_ANSE_AMSTERDAM';
    $req_time = $req_time || 1398426904;
    $backend = $backend || 'Dummy';
      
    my $testCircuit = PHEDEX::File::Download::Circuits::Circuit->new();
    $testCircuit->setNodes($from, $to);
    $testCircuit->registerRequest($backend, $life, $req_bandwidth);
    $testCircuit->{REQUEST_TIME} = $req_time;
           
    return $testCircuit;
}

sub createEstablishedCircuit {
    my ($est_time, $from_ip, $to_ip, $al_bandwidth, $req_time, $backend, $from, $to, $life, $req_bandwidth) = @_;
    
    $from_ip = $from_ip || '192.168.0.1';    
    $to_ip = $to_ip || '192.168.0.2';
    
    $est_time = $est_time || 1398426910;
    
    my $testCircuit = createRequestingCircuit($req_time, $backend, $from, $to, $life, $req_bandwidth);
    $testCircuit->registerEstablished($from_ip, $to_ip, $al_bandwidth);
    $testCircuit->{ESTABLISHED_TIME} = $est_time;
     
    return $testCircuit;
}

sub createOfflineCircuit {
    my ($time) = @_;    
    my $testCircuit = createEstablishedCircuit();
    $testCircuit->registerTakeDown();
    $testCircuit->{LAST_STATUS_CHANGE} = $time if $time;    
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