package PHEDEX::File::Download::Circuits::Helpers::Utils::Utils;

use strict;
use warnings;

use Scalar::Util qw(blessed);

use PHEDEX::File::Download::Circuits::Helpers::Utils::UtilsConstants;
use POSIX qw(strftime);
use Time::HiRes 'gettimeofday';

use base 'Exporter';
our @EXPORT = qw(
                compareObject
                checkPort determineAddressType replaceHostnameInURL
                getPath getFormattedTime
                checkArguments
                mytimeofday
                );

########################Generic functions########################
# Recursive compare of two PERL objects
sub compareObject {
    my ($object1, $object2) = @_;

    # Not equal if one's defined and the other isn't
    return 0 if (!defined $object1 == defined $object2);
    # Equal if both aren't defined
    return 1 if (!defined $object1 && !defined $object2);

    my ($dref1, $dref2) = (ref($object1), ref($object2));
    # Not equal if referenced types don't match
    return 0 if $dref1 ne $dref2;

    # Return simple comparison for variables passed by values
    return $object1 eq $object2 if ($dref1 eq '');

    if ($dref1 eq 'SCALAR' || $dref1 eq 'REF') {
        return &compareObject(${$object1}, ${$object2});
    } elsif ($dref1 eq 'ARRAY'){
        # Not equal if array size differs
        return 0 if ($#{$object1} != $#{$object1});
        # Go through all the items - order counts!
        for my $i (0 .. @{$object1}) {
            return 0 if ! &compareObject($object1->[$i], $object2->[$i]);
        }
    } elsif ($dref1 eq 'HASH' || defined blessed($object1)) {
        # Not equal if they don't have the same number of keys
        return 0 if (scalar keys (%{$object1}) != scalar keys (%{$object2}));
        # Go through all the items
        foreach my $key (keys %{$object1}) {
            return 0 if ! &compareObject($object1->{$key}, $object2->{$key});
        }
    }

    # Equal, if we get to here
    return 1;
}

########################IP helper functions########################

# Determines if the specified port is in the 1024-65535 range (Exclude system ports)
sub checkPort {
    my ($port) = @_;
    return !defined $port || $port<1024 || $port >= 65536 ? PORT_INVALID : PORT_VALID;
}

# Determines if the specified attribute is an IPv4 address, a valid hostname or neither
sub determineAddressType {
    my ($hostname) = @_;

    return  !defined $hostname ? ADDRESS_INVALID :
            $hostname =~ ('^'.REGEX_VALID_IPV4.'$') ?
            ADDRESS_IPv4 : $hostname =~ ('^'.REGEX_VALID_IPV6.'$') ?
            ADDRESS_IPv6 : $hostname =~ ('^'.REGEX_VALID_HOSTNAME.'$') ?
            ADDRESS_HOSTNAME : ADDRESS_INVALID;
}

# Replaces the IP or hostname and port in the source URL with the ones provided
# If it's unable to find a valid hostname/ip to replace, it will return undef
sub replaceHostnameInURL {
    my ($url, $protocol, $newIP, $newPort) = @_;

    # Don't attempt to do anything if the provided parameters are invalid
    return if (
                !defined $url || !defined $protocol ||
                determineAddressType($newIP) eq ADDRESS_INVALID ||
                defined $newPort && checkPort($newPort) eq PORT_INVALID
              );

    # Find the hostname or IP in the URL
    my $urlMatch = "^($protocol:\/\/)(".REGEX_VALID_IPV4."|".REGEX_VALID_IPV6."|".REGEX_VALID_HOSTNAME.")((:".REGEX_VALID_PORT.")?)(\/.*)\$";
    my @matchExtract = ($url =~ m/$urlMatch/);

    # Special case where IPv6 is given with a port number since it has to be matched to [ip]:port
    if (! defined $matchExtract[1]) {
        my $validIpv6AddressRegexWPort = '(\['.REGEX_VALID_IPV6.'\])(:'.REGEX_VALID_PORT.')'.'(\/.*)';
        $urlMatch = "^($protocol:\/\/)$validIpv6AddressRegexWPort\$";
        @matchExtract = ($url =~ m/$urlMatch/);
    }

    my ($extractedHost, $extractedPort, $extractedPath) = ($matchExtract[1], $matchExtract[@matchExtract - 2], $matchExtract[@matchExtract - 1]);

    $newPort = $extractedPort if (checkPort($newPort) eq PORT_INVALID && checkPort($extractedPort));

    return if (!defined $extractedHost);

    my $newURL = "$protocol://"
                    .
                    (determineAddressType($newIP) eq ADDRESS_IPv4 || determineAddressType($newIP) eq ADDRESS_IPv6 && checkPort($newPort) eq PORT_INVALID ? "$newIP" : "[$newIP]")
                    .
                    (checkPort($newPort) eq PORT_VALID ? ":".$newPort : "").$extractedPath;

    return $newURL;
}

# Returns the link name in the form of Node1-to-Node2 or Node1-Node2 from two given nodes
sub getPath {
    my ($nodeA, $nodeB, $bidirectional) = @_;
    return undef if (! defined $nodeA || ! defined $nodeB || $nodeA eq $nodeB);

    my $link = $bidirectional ? "-":"-to-";
    my $name;
    
    if ($bidirectional) {
        $name = ($nodeA cmp $nodeB) == -1 ? $nodeA.$link.$nodeB : $nodeB.$link.$nodeA;
    } else {
        $name = $nodeA.$link.$nodeB;
    }

    return $name;
}

# Generates a human readable date and time - mostly used when saving, in the state file name
sub getFormattedTime{
    my ($time, $includeMilis) = @_;

    return if ! defined $time;

    my $milis = '';

    if ($includeMilis) {
        $milis = sprintf("%.4f", $time - int($time));
        $milis  =~ s/^.//;
    }

    return strftime('%Y%m%d-%Hh%Mm%S', gmtime(int($time))).$milis;
}

sub checkArguments {
    # Check to see if the arguments are in order
    foreach my $arg (@_) {
        return undef if (! defined $arg);
    }
    return 1;
}

# High-resolution timing (copied from PhEDEx::Timing to avoid inclusion of PhEDEx packages)
sub mytimeofday {
    return scalar (&gettimeofday());
}