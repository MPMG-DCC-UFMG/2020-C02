#! /bin/bash


for f in ${BASH_ARGV[*]} ; do
    { docker run -v "$(python3 json_maker.py $f --output-folder):/var/youtube-crawler/" --rm -it $(docker images | grep 'mp_yt' | head -1 | cut -d' ' -f 1) python3 /home/mp/coletor-youtube/main.py -d "$(python3 json_maker.py $f)" ; }
    # -e LANG=C.UTF-8
done
