#!/usr/bin/perl
my $sum=0;
while (<STDIN>){
    my $line = $_;
    chomp ($line);
    $sum=${sum}+${line};
}
print $sum;