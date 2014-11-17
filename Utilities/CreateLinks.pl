#!/usr/bin/perl

use strict;
use warnings;

use Getopt::Long;
use File::Path;
use File::Copy;

# Define all the master files here
# The CHKSUM needs to be input by hand. For very large files it takes too long to compute
my $masterFiles;
$masterFiles->[0]->{NAME} = "/data1/master.root";
$masterFiles->[0]->{SIZE} = -s $masterFiles->[0]->{NAME};
$masterFiles->[0]->{CHKSUM} = "ADD_CRC_HERE";

$masterFiles->[1]->{NAME} = "/data2/master.root";
$masterFiles->[1]->{SIZE} = -s $masterFiles->[1]->{NAME};
$masterFiles->[1]->{CHKSUM} = "ADD_CRC_HERE";

$masterFiles->[2]->{NAME} = "/data3/master.root";
$masterFiles->[2]->{SIZE} = -s $masterFiles->[2]->{NAME};
$masterFiles->[2]->{CHKSUM} = "ADD_CRC_HERE";

my $masterCount = scalar @{$masterFiles};

# Define the booth and node for which we're going to create the links
my $booth = "caltech";
my $node = "sc1";
my $interfaces = 2;

my $storeLocation = "/data/ANSE/store";
my $blocksPerDataset = 2;
my $filesPerBlock = 5;

my $createMasterFiles = 0;
my $masterFileSize = "20K";

my $createSymlinks = 0;
my $help = 0;
my $clean = 0;

GetOptions ("booth=s"           => \$booth,
            "node=s"            => \$node,
            "interfaces=i"      => \$interfaces,
            "storeLocation=s"   => \$storeLocation,
            "blocksPerDataset=i"=> \$blocksPerDataset,
            "filesPerBlock=i"   => \$filesPerBlock,
            "createMasterFiles" => \$createMasterFiles,
            "masterFileSize=s"  => \$masterFileSize,
            "createSymlinks"    => \$createSymlinks,
            "help"              => \$help,
            "clean"             => \$clean);

if ($help) {
    print "Available options are:\n";
    print "\t -booth                Defines the booth ID (ex. caltech)\n";
    print "\t -node                 Defines the node ID (ex. sc1)\n";
    print "\t -interfaces           Defines the number of interfaces on a node\n";
    print "\t -storeLocation        Defines the place where the symlinks will be put (ex /data/anse/store)\n";
    print "\t -blocksPerDataset     Defines the number of blocks in a datast\n";
    print "\t -filesPerBlock        Defines the number of files in a block\n";
    print "\t -createMasterFiles    (flag) Creates the master files (in /data1, /data2/, /data3\n";
    print "\t -masterFileSize       Overrides the default master file size (of 20K). Specify in dd format\n";
    print "\t -createSymlinks       (flag) Creates the symlinks\n";
    print "\t -clean                (flag) Removes master files and all store locations";
    exit 0;
}

if ($clean) {
    print "Cleaning the master files\n";
    for (my $i = 0; $i < $masterCount; $i++) {
        unlink $masterFiles->[$i]->{NAME};
    }
    
    print "Cleaning store\n";
    File::Path::rmtree($storeLocation, 1, 1);
    
    exit 0;
}

# Create the master files if someone asks
if ($createMasterFiles) {
    for (my $i = 0; $i < $masterCount; $i++) {
        print "Creating master file $masterFiles->[$i]->{NAME} (size: $masterFileSize)\n";
        system("dd if=/dev/urandom of=$masterFiles->[$i]->{NAME} bs=$masterFileSize count=1");
        $masterFiles->[$i]->{SIZE} = $masterFileSize;
    }
}

my (@datasets);

# For all the (network) interfaces
for (my $interface = 1; $interface <= $interfaces; $interface++) {
    
    # Set dataset name
    my $datasetName = "dataset-$booth-$node-$interface";
    print "Creating dataset: $datasetName\n";
    
    my $dataset;
    $dataset->{NAME} = $datasetName;
    
    push (@datasets, $dataset);
    
    for (my $blockCounter = 1; $blockCounter <= $blocksPerDataset; $blockCounter++) {
        
        # Set block name;
        my $blockName = substr("0000".$blockCounter, -4);
        print "Creating block $blockName in dataset $datasetName\n";
        
        my $block;
        $block->{NAME} = $blockName;
        $block->{DATASET} = $dataset;
        
        # Add the block to the dataset
        push (@{$dataset->{BLOCKS}}, $block);
        
        # Also compute where the files will land
        $block->{PATH} = "$storeLocation/$datasetName/block-$blockName";
        
        for (my $fileCounter = 0; $fileCounter < $filesPerBlock; $fileCounter++) {
            my $fileID = substr("00000000".($fileCounter + 1), -8);
            my $masterIndex = ($fileCounter % $masterCount);
            my $fileName = "file-$fileID-ctrl".($masterIndex + 1).".root";
            
            my $file;
            $file->{MASTER} = $masterFiles->[$masterIndex]->{NAME};
            $file->{SIZE}   = $masterFiles->[$masterIndex]->{SIZE};
            $file->{CHKSUM} = $masterFiles->[$masterIndex]->{CHKSUM};
            $file->{NAME}   = $fileName;
            $file->{BLOCK}   = $block;
            $file->{DATASET}   = $dataset;
            
            push (@{$block->{FILES}}, $file);
        }
    }
}

foreach my $dataset (@datasets) {
    foreach my $block (@{$dataset->{BLOCKS}}) {
        
        if ($createSymlinks) {
            print "Creating $block->{PATH}\n";
            File::Path::make_path($block->{PATH}, { error => \my $err});
        }
        
        foreach my $file (@{$block->{FILES}}) {
            symlink($file->{MASTER}, "$block->{PATH}/$file->{NAME}") if $createSymlinks;
            print "$block->{PATH}/$file->{NAME} -> $file->{MASTER}\n";
        }
    }
}

exit 0;