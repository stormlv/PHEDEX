package PHEDEX::Web::API::SiteDataInfo;
use strict;
use warnings;
use PHEDEX::Web::Util;
use PHEDEX::Web::SQL;

sub duration { return 0; }
sub invoke
{
  my ($core,%args) = @_;
  &checkRequired(\%args, 'SITENAME');
  return PHEDEX::Web::SQL::SiteDataInfo(@_);
}

1;
