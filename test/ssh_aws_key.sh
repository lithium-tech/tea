#!/bin/bash

EXPORTS="export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID; export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY"

OPTIONS=""
while (("$#")); do
        case $1 in
        -4|-6|-A|-a|-C|-f|-G|-g|-K|-k|-M|-N|-n|-q|-s|-T|-t|-V|-v|-X|-x|-Y|-y)
                OPTIONS="$OPTIONS $1"
                shift || break
                ;;
        -*)
                OPTIONS="$OPTIONS $1 $2"
                shift
                shift || break
                ;;
        *)
                break
                ;;
        esac
done

DST=$1
shift

ssh $OPTIONS $DST "$EXPORTS; $@"
