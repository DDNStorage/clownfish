dnl Process this file with autoconf to produce a configure script.
AC_INIT([clownfish], [m4_esyscmd(./version-gen.sh)], [lixi@ddn.com], [clownfish])
AC_CONFIG_SRCDIR(./pyclownfish/clownfish.py)
AC_CONFIG_HEADERS(config.h)

AM_INIT_AUTOMAKE([tar-pax dist-bzip2 foreign])
AM_EXTRA_RECURSIVE_TARGETS([check_clean])
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])
AC_LANG(C)

AC_PREFIX_DEFAULT([/usr])

AC_PROG_CC
AC_PROG_CPP
AC_PROG_INSTALL
AC_PROG_LN_S
AC_PROG_MAKE_SET
AM_PROG_CC_C_O
AM_CONDITIONAL(COMPILER_IS_GCC, test "x$GCC" = "xyes")

AC_DISABLE_STATIC
AC_PROG_LIBTOOL
AC_PROG_LEX
AC_PROG_YACC
PKG_PROG_PKG_CONFIG

#
# MB_CHECK_FILE
#
# Check for file existance even when cross compiling
# $1 - file to check
# $2 - do 'yes'
# $3 - do 'no'
#
AC_DEFUN([MB_CHECK_FILE], [
AS_VAR_PUSHDEF([mb_file], [mb_cv_file_$1])dnl
AC_CACHE_CHECK([for $1], mb_file, [
AS_IF([test -r "$1"],
	[AS_VAR_SET(mb_file, [yes])],
	[AS_VAR_SET(mb_file, [no])])
])
AS_IF([test AS_VAR_GET(mb_file) = yes], [$2], [$3])[]dnl
AS_VAR_POPDEF([mb_file])dnl
]) # MB_CHECK_FILE

#
# MB_ARG_REPLACE_PATH(PACKAGE, PATH)
#
AC_DEFUN([MB_ARG_REPLACE_PATH], [
new_configure_args=
eval set -- $ac_configure_args
for arg; do
	case $arg in
		--with-[$1]=*)
			arg=--with-[$1]=[$2] ;;
		*\'*)
			arg=$(printf %s\n ["$arg"] | \
				sed "s/'/'\\\\\\\\''/g") ;;
	esac
	dnl AS_VAR_APPEND([new_configure_args], [" '$arg'"])
	new_configure_args="$new_configure_args '$arg'"
done
ac_configure_args=$new_configure_args
])

#
# __MB_ARG_CANON_PATH
#
# this is the work-horse of the next function
#
AC_DEFUN([__MB_ARG_CANON_PATH], [
	[$3]=$(readlink -f $with_$2)
	MB_ARG_REPLACE_PATH([$1], $[$3])
])

#
# MB_ARG_CANON_PATH
#
# a front-end for the above function that transforms - and . in the
# PACKAGE portion of --with-PACKAGE into _ suitable for variable names
#
AC_DEFUN([MB_ARG_CANON_PATH], [
	__MB_ARG_CANON_PATH([$1], m4_translit([$1], [-.], [__]), [$2])
])

dnl We may be able to use older versions, but I have not verified that
# -------- check for distro version --------
AC_MSG_CHECKING([for distro version])
DISTRO=$(sh detect-distro.sh)
DISTRO_NAME=$(echo $DISTRO | awk -F '-' '{print $1}')
if [[[ "$DISTRO_NAME" != "rhel" ]]]; then
        AC_MSG_ERROR([$DISTRO_NAME is not a supported distro.])
fi
DISTRO_RELEASE=$(echo $DISTRO | awk -F 'rhel-' '{print $2}' | awk -F '.' '{print $1}')
AC_MSG_RESULT([$DISTRO_RELEASE])
AC_SUBST(DISTRO_RELEASE)

# ------- check for python --------
AC_CHECK_PROGS([PYTHON], [python], [])
if test "x$PYTHON" = "x"; then
	AC_MSG_ERROR([Python is needed for tests. Install python please.])
fi

# ------- check for pep8 --------
AC_CHECK_PROGS([PEP8], [pep8], [])
if test "x$PEP8" = "x"; then
	AC_MSG_ERROR([pep8 is needed to check python coding style. Install pep8 please.])
fi

# ------- check for pylint --------
AC_CHECK_PROGS([PYLINT], [pylint], [])
if test "x$PYLINT" = "x"; then
	AC_MSG_ERROR([pylint is needed to check python coding style. Install pylint please.])
fi

# ------- check for protoc --------
AC_CHECK_PROGS([PROTOC], [protoc], [])
if test "x$PROTOC" = "x"; then
	AC_MSG_ERROR([protoc is needed to compile Protobuf file. Install protoc please.])
fi

# -------- check for cached ISO --------
AC_MSG_CHECKING([for cached ISO])
AC_ARG_WITH([cached-iso],
	AC_HELP_STRING([--with-cached-iso=path],
		       [set path to cached ISO]),
	[MB_ARG_CANON_PATH([cached-iso], [CACHED_ISO_PATH])],
	[])
AC_MSG_RESULT([$CACHED_ISO_PATH])
AC_SUBST(CACHED_ISO_PATH)

AS_IF([test "x$CACHED_ISO_PATH" != "x"],
      [MB_CHECK_FILE([$CACHED_ISO_PATH], [],
                     [AC_MSG_ERROR([cached ISO dir $CACHED_ISO_PATH could not be found.])])])

CLOWNFISH_RELEASE="1"
AC_DEFINE_UNQUOTED(RELEASE, "$CLOWNFISH_RELEASE", [release info] )
AC_SUBST(CLOWNFISH_RELEASE)

# for exporting to spec file
AC_SUBST(ac_configure_args)
AC_CONFIG_FILES([Makefile
                 clownfish.spec
                 pybuild/Makefile])
AC_OUTPUT
