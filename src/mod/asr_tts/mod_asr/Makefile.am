include $(top_srcdir)/build/modmake.rulesam
MODNAME=mod_asr

mod_LTLIBRARIES = mod_asr.la
mod_asr_la_SOURCES  = mod_asr.c asr_provider.c asr_session.c asr_ws_client.c asr_rest_client.c provider_aliyun.c
mod_asr_la_CFLAGS   = $(AM_CFLAGS)
mod_asr_la_LIBADD   = $(switch_builddir)/libfreeswitch.la
mod_asr_la_LDFLAGS  = -avoid-version -module -no-undefined -shared
