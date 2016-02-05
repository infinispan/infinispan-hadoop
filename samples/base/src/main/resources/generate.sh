#!/usr/bin/env bash
ruby -e 'a=STDIN.readlines;ARGV[0].to_i.times do;b=[];(rand(12)+5).times do; b << a[rand(a.size)].chomp end; puts b.join(" "); end' -- "$@" < /usr/share/dict/cracklib-words > file.txt
