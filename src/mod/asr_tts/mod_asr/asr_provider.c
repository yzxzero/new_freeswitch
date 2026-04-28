/*
 * asr_provider.c -- Provider registration and lookup framework
 */

#include "mod_asr.h"

/* asr_globals is defined in mod_asr.c */

switch_status_t asr_provider_register(asr_provider_interface_t *provider)
{
	if (!provider || zstr(provider->name)) {
		return SWITCH_STATUS_FALSE;
	}

	switch_mutex_lock(asr_globals.mutex);
	switch_core_hash_insert(asr_globals.providers, provider->name, provider);
	switch_mutex_unlock(asr_globals.mutex);

	switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "ASR provider registered: %s\n", provider->name);

	return SWITCH_STATUS_SUCCESS;
}

asr_provider_interface_t *asr_provider_find(const char *name)
{
	asr_provider_interface_t *provider = NULL;

	if (zstr(name)) {
		return NULL;
	}

	switch_mutex_lock(asr_globals.mutex);
	provider = switch_core_hash_find(asr_globals.providers, name);
	switch_mutex_unlock(asr_globals.mutex);

	return provider;
}

void asr_provider_list(switch_stream_handle_t *stream)
{
	switch_hash_index_t *hi;
	asr_provider_interface_t *provider;

	switch_mutex_lock(asr_globals.mutex);
	for (hi = switch_core_hash_first(asr_globals.providers); hi; hi = switch_core_hash_next(&hi)) {
		switch_core_hash_this(hi, NULL, NULL, (void **) &provider);
		stream->write_function(stream, "  %-20s  open=%p  feed=%p  get_results=%p\n",
							   provider->name,
							   (void *) (uintptr_t) provider->open,
							   (void *) (uintptr_t) provider->feed,
							   (void *) (uintptr_t) provider->get_results);
	}
	switch_mutex_unlock(asr_globals.mutex);
}
