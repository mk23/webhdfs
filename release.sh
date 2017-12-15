#!/bin/bash -e

BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [ "${BRANCH}" = "HEAD" ] ; then
	echo "Cannot release on detached HEAD.  Please switch to a branch."
	exit 1
fi

read -p "Would you like to commit ${BRANCH} branch? [y/N] " -r
if [[ $REPLY =~ ^[Yy]$ ]] ; then
	COMMIT=--commit
fi

echo
(
	/usr/bin/curl -L -s https://raw.github.com/mk23/sandbox/master/misc/release.py ||
	echo 'raise Exception("unable to load release.py")'
) |
	exec /usr/bin/env python2.7 - ${COMMIT} \
		--release=xenial \
		--append=-upstream1 \
		--extra lib/__init__.py "__version__ = '{version}'" \
		"$@"

if [ -n "${COMMIT}" ] ; then
	echo
	TAG=$(git describe --abbrev=0 --tags)
	echo "Created tag ${TAG}"

	for REMOTE in $(git remote -v | cut -f1 | uniq) ; do
		read -p "Would you like to push to ${REMOTE}? [y/N] " -r
		if [[ $REPLY =~ ^[Yy]$ ]] ; then
		    git push "${REMOTE}" "${BRANCH}" "${TAG}"
		fi
	done
fi
