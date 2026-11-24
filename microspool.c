/*
 * microspool — миниатюрный сервер, совместимый с подмножеством Spoolman REST API,
 * для FlashForge AD5M (Klipper/Moonraker/HelixScreen), 128 МБ ОЗУ.
 *
 * Один процесс, один поток, event loop на poll(). JSON — cJSON (vendor/).
 * SHA-1 и base64 (для WebSocket handshake) написаны вручную ниже.
 *
 * Автор комментариев и README — по-русски, по требованию проекта.
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdarg.h>
#include <time.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include <fcntl.h>
#include <math.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <poll.h>
#include <sys/stat.h>

#include "vendor/cJSON.h"

/* Сгенерировано tools/embed_ui.py из ui/index.html (gzip -9); см. Makefile.
   UI_INDEX_GZ / UI_INDEX_GZ_LEN. */
#include "build/ui_gz.h"

/* ------------------------------------------------------------------------ */
/* Константы / лимиты                                                       */
/* ------------------------------------------------------------------------ */

#define MAX_VENDORS     64
#define MAX_FILAMENTS   256
#define MAX_SPOOLS      256

#define STR_CAP         257    /* строковые поля: <=256 байт + NUL */
#define EXTRA_CAP       1024   /* буфер под сериализованный JSON "extra" */

#define BODY_LIMIT      16384  /* тело запроса <= 16 КБ, иначе 413 */
#define NUM_ABS_MAX     1e9    /* numbers in requests: finite and |x| <= 1e9 (1000 t of filament) */
#define HEADER_LIMIT    8192   /* защита от неограниченного роста буфера заголовков */

/* Connections. Moonraker keeps one websocket open all the time and makes short
   HTTP requests; the web UI is reachable from the LAN without a login, so LAN
   clients must never be able to take the slots Moonraker needs. */
#define MAX_CONNS             32
#define LOCAL_RESERVED_CONNS  4     /* slots LAN clients can never take */
#define MAX_CONNS_PER_PEER    8     /* per LAN address */
#define MAX_REMOTE_WS         4     /* websockets from LAN addresses, in total */
#define EVICT_MIN_AGE_S       1.0   /* a LAN request still incomplete after this may be evicted */
#define LISTEN_BACKLOG        32

#define HTTP_REQUEST_DEADLINE_S 10  /* accept -> response sent, whatever the client does (slowloris) */
#define WS_PING_INTERVAL_S    20      /* по умолчанию; -k задаёт своё, -k 0 отключает пинги сервера */
#define WS_TIMEOUT_PINGS      3     /* websocket closed after this many ping intervals without any byte from the peer */

#define BUF_KEEP_CAP          16384             /* per-slot buffers up to this size are reused */
#define WS_TX_LIMIT           (256 * 1024)      /* unsent bytes per websocket peer before it is dropped */
#define TX_TOTAL_LIMIT        (8 * 1024 * 1024) /* all send buffers together */
#define USE_FLUSH_INTERVAL_S  30

/* Версия Spoolman API, которую мы изображаем в /api/v1/info (клиенты сверяют), и собственная версия. */
static int g_ws_ping_interval = WS_PING_INTERVAL_S;
static const char *SERVER_VERSION = "0.22.1";
#ifndef MICROSPOOL_VERSION
#define MICROSPOOL_VERSION "0.1.0"
#endif

/* ------------------------------------------------------------------------ */
/* Глобальное состояние процесса                                            */
/* ------------------------------------------------------------------------ */

static const char *g_listen_addr = "127.0.0.1";
static int         g_listen_port = 7912;
static const char *g_data_path   = "./microspool.json";
static int         g_verbose     = 0;
static int         g_ui_enabled  = 1;   /* -U отключает встроенный веб-интерфейс */

/* Host / Origin allowlists (see request_allowed). */
#define MAX_ALLOW_NAMES 16
static const char *g_allow_hosts[MAX_ALLOW_NAMES];    /* -H name (or -H '*' for any) */
static int         g_allow_hosts_n = 0;
static int         g_allow_any_host = 0;
static const char *g_allow_origins[MAX_ALLOW_NAMES];  /* -o http://host[:port] */
static int         g_allow_origins_n = 0;
static char        g_hostname[256];                   /* gethostname(), lower case */

static volatile sig_atomic_t g_should_exit = 0;

static void log_v(const char *fmt, ...) {
    if (!g_verbose) return;
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fputc('\n', stderr);
}

/* ------------------------------------------------------------------------ */
/* Время                                                                    */
/* ------------------------------------------------------------------------ */

/* ISO 8601 UTC со смещением: 2026-09-14T19:30:00+00:00 */
static void now_iso(char *buf, size_t bufsz) {
    time_t t = time(NULL);
    struct tm tmv;
    /* strftime instead of snprintf("%04d..."): no -Wformat-truncation, and a
       year past 9999 just yields an empty string instead of a cut-off one. */
    if (gmtime_r(&t, &tmv) == NULL || strftime(buf, bufsz, "%Y-%m-%dT%H:%M:%S+00:00", &tmv) == 0) {
        if (bufsz) buf[0] = 0;
    }
}

static double mono_now(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

/* ------------------------------------------------------------------------ */
/* SHA-1 (простая явная реализация, без побитовых трюков с endianness)      */
/* ------------------------------------------------------------------------ */

typedef struct {
    uint32_t state[5];
    uint64_t bitlen;
    unsigned char buf[64];
    size_t buflen;
} sha1_ctx_t;

static void sha1_transform(uint32_t state[5], const unsigned char block[64]) {
    uint32_t w[80];
    for (int i = 0; i < 16; i++) {
        w[i] = ((uint32_t)block[i*4] << 24) | ((uint32_t)block[i*4+1] << 16) |
               ((uint32_t)block[i*4+2] << 8) | ((uint32_t)block[i*4+3]);
    }
    for (int i = 16; i < 80; i++) {
        uint32_t v = w[i-3] ^ w[i-8] ^ w[i-14] ^ w[i-16];
        w[i] = (v << 1) | (v >> 31);
    }
    uint32_t a = state[0], b = state[1], c = state[2], d = state[3], e = state[4];
    for (int i = 0; i < 80; i++) {
        uint32_t f, k;
        if (i < 20)      { f = (b & c) | ((~b) & d);        k = 0x5A827999u; }
        else if (i < 40) { f = b ^ c ^ d;                    k = 0x6ED9EBA1u; }
        else if (i < 60) { f = (b & c) | (b & d) | (c & d);  k = 0x8F1BBCDCu; }
        else             { f = b ^ c ^ d;                    k = 0xCA62C1D6u; }
        uint32_t t = ((a << 5) | (a >> 27)) + f + e + k + w[i];
        e = d; d = c; c = (b << 30) | (b >> 2); b = a; a = t;
    }
    state[0] += a; state[1] += b; state[2] += c; state[3] += d; state[4] += e;
}

static void sha1_init(sha1_ctx_t *ctx) {
    ctx->state[0] = 0x67452301u;
    ctx->state[1] = 0xEFCDAB89u;
    ctx->state[2] = 0x98BADCFEu;
    ctx->state[3] = 0x10325476u;
    ctx->state[4] = 0xC3D2E1F0u;
    ctx->bitlen = 0;
    ctx->buflen = 0;
}

static void sha1_update(sha1_ctx_t *ctx, const unsigned char *data, size_t len) {
    ctx->bitlen += (uint64_t)len * 8;
    while (len > 0) {
        size_t n = 64 - ctx->buflen;
        if (n > len) n = len;
        memcpy(ctx->buf + ctx->buflen, data, n);
        ctx->buflen += n; data += n; len -= n;
        if (ctx->buflen == 64) {
            sha1_transform(ctx->state, ctx->buf);
            ctx->buflen = 0;
        }
    }
}

static void sha1_final(sha1_ctx_t *ctx, unsigned char digest[20]) {
    uint64_t bitlen = ctx->bitlen;
    ctx->buf[ctx->buflen++] = 0x80;
    if (ctx->buflen > 56) {
        while (ctx->buflen < 64) ctx->buf[ctx->buflen++] = 0;
        sha1_transform(ctx->state, ctx->buf);
        ctx->buflen = 0;
    }
    while (ctx->buflen < 56) ctx->buf[ctx->buflen++] = 0;
    for (int i = 0; i < 8; i++) ctx->buf[56+i] = (unsigned char)(bitlen >> (56 - 8*i));
    sha1_transform(ctx->state, ctx->buf);
    for (int i = 0; i < 5; i++) {
        digest[i*4]   = (unsigned char)(ctx->state[i] >> 24);
        digest[i*4+1] = (unsigned char)(ctx->state[i] >> 16);
        digest[i*4+2] = (unsigned char)(ctx->state[i] >> 8);
        digest[i*4+3] = (unsigned char)(ctx->state[i]);
    }
}

/* ------------------------------------------------------------------------ */
/* base64 (только кодирование, нужно для Sec-WebSocket-Accept)              */
/* ------------------------------------------------------------------------ */

static const char b64tab[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/* out должен вмещать 4*ceil(inlen/3)+1 байт */
static void base64_encode(const unsigned char *in, size_t inlen, char *out) {
    size_t i = 0, o = 0;
    while (i + 3 <= inlen) {
        uint32_t n = ((uint32_t)in[i] << 16) | ((uint32_t)in[i+1] << 8) | in[i+2];
        out[o++] = b64tab[(n >> 18) & 0x3F];
        out[o++] = b64tab[(n >> 12) & 0x3F];
        out[o++] = b64tab[(n >> 6) & 0x3F];
        out[o++] = b64tab[n & 0x3F];
        i += 3;
    }
    size_t rem = inlen - i;
    if (rem == 1) {
        uint32_t n = (uint32_t)in[i] << 16;
        out[o++] = b64tab[(n >> 18) & 0x3F];
        out[o++] = b64tab[(n >> 12) & 0x3F];
        out[o++] = '='; out[o++] = '=';
    } else if (rem == 2) {
        uint32_t n = ((uint32_t)in[i] << 16) | ((uint32_t)in[i+1] << 8);
        out[o++] = b64tab[(n >> 18) & 0x3F];
        out[o++] = b64tab[(n >> 12) & 0x3F];
        out[o++] = b64tab[(n >> 6) & 0x3F];
        out[o++] = '=';
    }
    out[o] = 0;
}

/* Sec-WebSocket-Accept = base64(SHA1(key + magic GUID)), RFC 6455 */
static void ws_compute_accept(const char *client_key, char *out_b64 /* >=32 байт */) {
    static const char *magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    char concat[512];
    snprintf(concat, sizeof concat, "%s%s", client_key, magic);
    unsigned char digest[20];
    sha1_ctx_t ctx;
    sha1_init(&ctx);
    sha1_update(&ctx, (const unsigned char *)concat, strlen(concat));
    sha1_final(&ctx, digest);
    base64_encode(digest, 20, out_b64);
}

/* ------------------------------------------------------------------------ */
/* Модель данных                                                            */
/* ------------------------------------------------------------------------ */

typedef struct {
    int  id;                 /* 0 = свободный слот */
    char registered[32];
    char name[STR_CAP];
    int  has_comment;              char comment[STR_CAP];
    int  has_empty_spool_weight;   double empty_spool_weight;
    int  has_external_id;          char external_id[STR_CAP];
    char extra[EXTRA_CAP];         /* сериализованный JSON-объект, по умолчанию "{}" */
} Vendor;

typedef struct {
    int  id;
    char registered[32];
    int  has_name;                 char name[STR_CAP];
    int  has_vendor_id;            int vendor_id;
    int  has_material;             char material[STR_CAP];
    int  has_price;                double price;
    double density;                /* обязательное, > 0 */
    double diameter;                /* обязательное, > 0 */
    int  has_weight;                double weight;
    int  has_spool_weight;          double spool_weight;
    int  has_article_number;        char article_number[STR_CAP];
    int  has_comment;               char comment[STR_CAP];
    int  has_settings_extruder_temp; int settings_extruder_temp;
    int  has_settings_bed_temp;      int settings_bed_temp;
    int  has_color_hex;              char color_hex[8];
    int  has_multi_color_hexes;      char multi_color_hexes[STR_CAP];
    int  has_multi_color_direction;  char multi_color_direction[32];
    int  has_external_id;            char external_id[STR_CAP];
    char extra[EXTRA_CAP];
} Filament;

typedef struct {
    int  id;
    char registered[32];
    int  has_first_used; char first_used[32];
    int  has_last_used;  char last_used[32];
    int  filament_id;    /* обязательное, FK */
    int  has_price;          double price;
    int  has_initial_weight; double initial_weight;
    int  has_spool_weight;   double spool_weight;
    double used_weight;      /* всегда присутствует, по умолчанию 0 */
    int  has_location; char location[STR_CAP];
    int  has_lot_nr;   char lot_nr[STR_CAP];
    int  has_comment;  char comment[STR_CAP];
    int  archived;     /* всегда присутствует, по умолчанию false */
    char extra[EXTRA_CAP];
} Spool;

static Vendor   g_vendors[MAX_VENDORS];
static Filament g_filaments[MAX_FILAMENTS];
static Spool    g_spools[MAX_SPOOLS];

static int g_next_vendor_id = 1;
static int g_next_filament_id = 1;
static int g_next_spool_id = 1;

static int g_data_dirty_immediate = 0;   /* нужен немедленный сброс на диск */
static int g_use_pending = 0;            /* есть отложенное изменение use */
static double g_last_use_flush_mono = 0; /* когда последний раз сбрасывали use */
static double g_use_flush_deadline = 0;  /* когда сбросить отложенное use */

/* ------------------------------------------------------------------------ */
/* Вспомогательные функции по сущностям                                    */
/* ------------------------------------------------------------------------ */

static Vendor *vendor_find(int id) {
    if (id <= 0) return NULL;
    for (int i = 0; i < MAX_VENDORS; i++) if (g_vendors[i].id == id) return &g_vendors[i];
    return NULL;
}
static Filament *filament_find(int id) {
    if (id <= 0) return NULL;
    for (int i = 0; i < MAX_FILAMENTS; i++) if (g_filaments[i].id == id) return &g_filaments[i];
    return NULL;
}
static Spool *spool_find(int id) {
    if (id <= 0) return NULL;
    for (int i = 0; i < MAX_SPOOLS; i++) if (g_spools[i].id == id) return &g_spools[i];
    return NULL;
}

static Vendor *vendor_free_slot(void) {
    for (int i = 0; i < MAX_VENDORS; i++) if (g_vendors[i].id == 0) return &g_vendors[i];
    return NULL;
}
static Filament *filament_free_slot(void) {
    for (int i = 0; i < MAX_FILAMENTS; i++) if (g_filaments[i].id == 0) return &g_filaments[i];
    return NULL;
}
static Spool *spool_free_slot(void) {
    for (int i = 0; i < MAX_SPOOLS; i++) if (g_spools[i].id == 0) return &g_spools[i];
    return NULL;
}

/* сортировка списков по id по возрастанию (для стабильного порядка выдачи) */
static int cmp_int(const void *a, const void *b) {
    int ia = *(const int *)a, ib = *(const int *)b;
    return (ia > ib) - (ia < ib);
}

/* ------------------------------------------------------------------------ */
/* Разбор входного JSON: типобезопасные геттеры                             */
/*                                                                          */
/* present = ключ есть в объекте; has = значение не null (и валидно).       */
/* Для PATCH: present==0 -> поле не трогаем; present && !has -> явный null  */
/* (сброс поля); present && has -> новое значение.                         */
/* Возврат: 0 = ок, -1 = ошибка типа/длины (вызывающий код отвечает 400).   */
/* ------------------------------------------------------------------------ */

static int jget_string(const cJSON *obj, const char *key, char *buf, size_t bufsz,
                        int *present, int *has) {
    cJSON *item = cJSON_GetObjectItemCaseSensitive(obj, key);
    *present = (item != NULL);
    *has = 0;
    if (!item) return 0;
    if (cJSON_IsNull(item)) return 0;
    if (!cJSON_IsString(item) || item->valuestring == NULL) return -1;
    size_t len = strlen(item->valuestring);
    if (len > bufsz - 1) return -1;
    memcpy(buf, item->valuestring, len + 1);
    *has = 1;
    return 0;
}

static int jget_number(const cJSON *obj, const char *key, double *out,
                        int *present, int *has) {
    cJSON *item = cJSON_GetObjectItemCaseSensitive(obj, key);
    *present = (item != NULL);
    *has = 0;
    if (!item) return 0;
    if (cJSON_IsNull(item)) return 0;
    if (!cJSON_IsNumber(item)) return -1;
    /* cJSON turns 1e999 into inf, which would pass every "> 0" check; also keeps
       llround() in jget_int well inside int range. */
    if (!isfinite(item->valuedouble) || fabs(item->valuedouble) > NUM_ABS_MAX) return -1;
    *out = item->valuedouble;
    *has = 1;
    return 0;
}

static int jget_int(const cJSON *obj, const char *key, int *out,
                     int *present, int *has) {
    double d;
    int rc = jget_number(obj, key, &d, present, has);
    if (rc != 0 || !*has) return rc;
    *out = (int)llround(d);
    return 0;
}

static int jget_bool(const cJSON *obj, const char *key, int *out,
                      int *present, int *has) {
    cJSON *item = cJSON_GetObjectItemCaseSensitive(obj, key);
    *present = (item != NULL);
    *has = 0;
    if (!item) return 0;
    if (cJSON_IsNull(item)) return 0;
    if (!cJSON_IsBool(item)) return -1;
    *out = cJSON_IsTrue(item) ? 1 : 0;
    *has = 1;
    return 0;
}

/* extra: объект произвольных строковых значений; храним как сериализованный
   компактный JSON-текст. present/has по тем же правилам, что и выше. Like
   Spoolman, every value must be a string (clients store JSON-encoded values
   in it, e.g. "\"PLA+\"" or "0.0400"). */
static int jget_extra(const cJSON *obj, char *buf, size_t bufsz,
                       int *present, int *has) {
    cJSON *item = cJSON_GetObjectItemCaseSensitive(obj, "extra");
    *present = (item != NULL);
    *has = 0;
    if (!item) return 0;
    if (cJSON_IsNull(item)) return 0;
    if (!cJSON_IsObject(item)) return -1;
    const cJSON *ch;
    cJSON_ArrayForEach(ch, item) if (!cJSON_IsString(ch) || ch->string == NULL) return -1;
    char *s = cJSON_PrintUnformatted(item);
    if (!s) return -1;
    size_t len = strlen(s);
    if (len > bufsz - 1) { free(s); return -1; }
    memcpy(buf, s, len + 1);
    free(s);
    *has = 1;
    return 0;
}

/* Strict UTF-8 check (no overlong forms, no surrogates, <= U+10FFFF). cJSON
   copies string bytes as they are, so invalid UTF-8 would be stored and later
   sent to clients that fail to decode it. */
static int utf8_valid(const unsigned char *s, size_t n) {
    size_t i = 0;
    while (i < n) {
        unsigned char c = s[i];
        if (c < 0x80) { i++; continue; }
        size_t len;
        uint32_t cp;
        if (c >= 0xC2 && c <= 0xDF)      { len = 2; cp = c & 0x1Fu; }
        else if (c >= 0xE0 && c <= 0xEF) { len = 3; cp = c & 0x0Fu; }
        else if (c >= 0xF0 && c <= 0xF4) { len = 4; cp = c & 0x07u; }
        else return 0;
        if (n - i < len) return 0;
        for (size_t k = 1; k < len; k++) {
            if ((s[i + k] & 0xC0) != 0x80) return 0;
            cp = (cp << 6) | (s[i + k] & 0x3Fu);
        }
        if (len == 3 && cp < 0x800) return 0;
        if (len == 4 && (cp < 0x10000 || cp > 0x10FFFF)) return 0;
        if (cp >= 0xD800 && cp <= 0xDFFF) return 0;
        i += len;
    }
    return 1;
}

/* ------------------------------------------------------------------------ */
/* Персистентность                                                          */
/* ------------------------------------------------------------------------ */

static cJSON *vendor_raw_json(const Vendor *v) {
    cJSON *o = cJSON_CreateObject();
    cJSON_AddNumberToObject(o, "id", v->id);
    cJSON_AddStringToObject(o, "registered", v->registered);
    cJSON_AddStringToObject(o, "name", v->name);
    if (v->has_comment) cJSON_AddStringToObject(o, "comment", v->comment);
    if (v->has_empty_spool_weight) cJSON_AddNumberToObject(o, "empty_spool_weight", v->empty_spool_weight);
    if (v->has_external_id) cJSON_AddStringToObject(o, "external_id", v->external_id);
    cJSON *extra = cJSON_Parse(v->extra[0] ? v->extra : "{}");
    cJSON_AddItemToObject(o, "extra", extra ? extra : cJSON_CreateObject());
    return o;
}

static cJSON *filament_raw_json(const Filament *f) {
    cJSON *o = cJSON_CreateObject();
    cJSON_AddNumberToObject(o, "id", f->id);
    cJSON_AddStringToObject(o, "registered", f->registered);
    if (f->has_name) cJSON_AddStringToObject(o, "name", f->name);
    if (f->has_vendor_id) cJSON_AddNumberToObject(o, "vendor_id", f->vendor_id);
    if (f->has_material) cJSON_AddStringToObject(o, "material", f->material);
    if (f->has_price) cJSON_AddNumberToObject(o, "price", f->price);
    cJSON_AddNumberToObject(o, "density", f->density);
    cJSON_AddNumberToObject(o, "diameter", f->diameter);
    if (f->has_weight) cJSON_AddNumberToObject(o, "weight", f->weight);
    if (f->has_spool_weight) cJSON_AddNumberToObject(o, "spool_weight", f->spool_weight);
    if (f->has_article_number) cJSON_AddStringToObject(o, "article_number", f->article_number);
    if (f->has_comment) cJSON_AddStringToObject(o, "comment", f->comment);
    if (f->has_settings_extruder_temp) cJSON_AddNumberToObject(o, "settings_extruder_temp", f->settings_extruder_temp);
    if (f->has_settings_bed_temp) cJSON_AddNumberToObject(o, "settings_bed_temp", f->settings_bed_temp);
    if (f->has_color_hex) cJSON_AddStringToObject(o, "color_hex", f->color_hex);
    if (f->has_multi_color_hexes) cJSON_AddStringToObject(o, "multi_color_hexes", f->multi_color_hexes);
    if (f->has_multi_color_direction) cJSON_AddStringToObject(o, "multi_color_direction", f->multi_color_direction);
    if (f->has_external_id) cJSON_AddStringToObject(o, "external_id", f->external_id);
    cJSON *extra = cJSON_Parse(f->extra[0] ? f->extra : "{}");
    cJSON_AddItemToObject(o, "extra", extra ? extra : cJSON_CreateObject());
    return o;
}

static cJSON *spool_raw_json(const Spool *s) {
    cJSON *o = cJSON_CreateObject();
    cJSON_AddNumberToObject(o, "id", s->id);
    cJSON_AddStringToObject(o, "registered", s->registered);
    if (s->has_first_used) cJSON_AddStringToObject(o, "first_used", s->first_used);
    if (s->has_last_used) cJSON_AddStringToObject(o, "last_used", s->last_used);
    cJSON_AddNumberToObject(o, "filament_id", s->filament_id);
    if (s->has_price) cJSON_AddNumberToObject(o, "price", s->price);
    if (s->has_initial_weight) cJSON_AddNumberToObject(o, "initial_weight", s->initial_weight);
    if (s->has_spool_weight) cJSON_AddNumberToObject(o, "spool_weight", s->spool_weight);
    cJSON_AddNumberToObject(o, "used_weight", s->used_weight);
    if (s->has_location) cJSON_AddStringToObject(o, "location", s->location);
    if (s->has_lot_nr) cJSON_AddStringToObject(o, "lot_nr", s->lot_nr);
    if (s->has_comment) cJSON_AddStringToObject(o, "comment", s->comment);
    cJSON_AddBoolToObject(o, "archived", s->archived);
    cJSON *extra = cJSON_Parse(s->extra[0] ? s->extra : "{}");
    cJSON_AddItemToObject(o, "extra", extra ? extra : cJSON_CreateObject());
    return o;
}

static void safe_copy_str(char *dst, size_t dstsz, const cJSON *item) {
    if (item && cJSON_IsString(item) && item->valuestring) {
        strncpy(dst, item->valuestring, dstsz - 1);
        dst[dstsz - 1] = 0;
    } else {
        dst[0] = 0;
    }
}

/* extra from the data file: a JSON object -> compact text, anything else -> "{}".
   Older versions accepted non-string values; they are turned into their JSON
   text as a string, which is what Spoolman stores. */
static void load_extra_raw(const cJSON *o, char *buf, size_t bufsz) {
    const cJSON *extra = cJSON_GetObjectItemCaseSensitive(o, "extra");
    cJSON *copy = cJSON_IsObject(extra) ? cJSON_Duplicate(extra, 1) : NULL;
    for (cJSON *ch = copy ? copy->child : NULL; ch; ) {
        cJSON *next = ch->next;
        if (!cJSON_IsString(ch) && ch->string) {
            char *t = cJSON_PrintUnformatted(ch);
            cJSON *str = cJSON_CreateString(t ? t : "null");
            free(t);
            if (str && !cJSON_ReplaceItemInObjectCaseSensitive(copy, ch->string, str)) cJSON_Delete(str);
        }
        ch = next;
    }
    char *s = copy ? cJSON_PrintUnformatted(copy) : NULL;
    cJSON_Delete(copy);
    if (s && strlen(s) < bufsz) memcpy(buf, s, strlen(s) + 1);
    else snprintf(buf, bufsz, "{}");
    free(s);
}

/* id from the data file: an integer in 1..INT_MAX-1 (so next_id = max+1 cannot overflow). */
static int load_id_raw(const cJSON *o, int *out) {
    const cJSON *id = cJSON_GetObjectItemCaseSensitive(o, "id");
    if (!cJSON_IsNumber(id)) return -1;
    double d = id->valuedouble;
    if (!(d >= 1 && d < (double)INT_MAX) || d != floor(d)) return -1;
    *out = (int)d;
    return 0;
}

static int load_vendor_raw(const cJSON *o, Vendor *v) {
    memset(v, 0, sizeof *v);
    if (load_id_raw(o, &v->id) != 0) return -1;
    safe_copy_str(v->registered, sizeof v->registered, cJSON_GetObjectItemCaseSensitive(o, "registered"));
    safe_copy_str(v->name, sizeof v->name, cJSON_GetObjectItemCaseSensitive(o, "name"));
    cJSON *c = cJSON_GetObjectItemCaseSensitive(o, "comment");
    if (c) { v->has_comment = 1; safe_copy_str(v->comment, sizeof v->comment, c); }
    cJSON *esw = cJSON_GetObjectItemCaseSensitive(o, "empty_spool_weight");
    if (cJSON_IsNumber(esw)) { v->has_empty_spool_weight = 1; v->empty_spool_weight = esw->valuedouble; }
    cJSON *ext = cJSON_GetObjectItemCaseSensitive(o, "external_id");
    if (ext) { v->has_external_id = 1; safe_copy_str(v->external_id, sizeof v->external_id, ext); }
    load_extra_raw(o, v->extra, sizeof v->extra);
    return 0;
}

static int load_filament_raw(const cJSON *o, Filament *f) {
    memset(f, 0, sizeof *f);
    if (load_id_raw(o, &f->id) != 0) return -1;
    safe_copy_str(f->registered, sizeof f->registered, cJSON_GetObjectItemCaseSensitive(o, "registered"));
    cJSON *name = cJSON_GetObjectItemCaseSensitive(o, "name");
    if (name) { f->has_name = 1; safe_copy_str(f->name, sizeof f->name, name); }
    cJSON *vid = cJSON_GetObjectItemCaseSensitive(o, "vendor_id");
    if (cJSON_IsNumber(vid)) { f->has_vendor_id = 1; f->vendor_id = vid->valueint; }
    cJSON *mat = cJSON_GetObjectItemCaseSensitive(o, "material");
    if (mat) { f->has_material = 1; safe_copy_str(f->material, sizeof f->material, mat); }
    cJSON *price = cJSON_GetObjectItemCaseSensitive(o, "price");
    if (cJSON_IsNumber(price)) { f->has_price = 1; f->price = price->valuedouble; }
    cJSON *density = cJSON_GetObjectItemCaseSensitive(o, "density");
    f->density = cJSON_IsNumber(density) ? density->valuedouble : 0;
    cJSON *diameter = cJSON_GetObjectItemCaseSensitive(o, "diameter");
    f->diameter = cJSON_IsNumber(diameter) ? diameter->valuedouble : 0;
    cJSON *weight = cJSON_GetObjectItemCaseSensitive(o, "weight");
    if (cJSON_IsNumber(weight)) { f->has_weight = 1; f->weight = weight->valuedouble; }
    cJSON *sw = cJSON_GetObjectItemCaseSensitive(o, "spool_weight");
    if (cJSON_IsNumber(sw)) { f->has_spool_weight = 1; f->spool_weight = sw->valuedouble; }
    cJSON *an = cJSON_GetObjectItemCaseSensitive(o, "article_number");
    if (an) { f->has_article_number = 1; safe_copy_str(f->article_number, sizeof f->article_number, an); }
    cJSON *cm = cJSON_GetObjectItemCaseSensitive(o, "comment");
    if (cm) { f->has_comment = 1; safe_copy_str(f->comment, sizeof f->comment, cm); }
    cJSON *set = cJSON_GetObjectItemCaseSensitive(o, "settings_extruder_temp");
    if (cJSON_IsNumber(set)) { f->has_settings_extruder_temp = 1; f->settings_extruder_temp = set->valueint; }
    cJSON *sbt = cJSON_GetObjectItemCaseSensitive(o, "settings_bed_temp");
    if (cJSON_IsNumber(sbt)) { f->has_settings_bed_temp = 1; f->settings_bed_temp = sbt->valueint; }
    cJSON *ch = cJSON_GetObjectItemCaseSensitive(o, "color_hex");
    if (ch) { f->has_color_hex = 1; safe_copy_str(f->color_hex, sizeof f->color_hex, ch); }
    cJSON *mch = cJSON_GetObjectItemCaseSensitive(o, "multi_color_hexes");
    if (mch) { f->has_multi_color_hexes = 1; safe_copy_str(f->multi_color_hexes, sizeof f->multi_color_hexes, mch); }
    cJSON *mcd = cJSON_GetObjectItemCaseSensitive(o, "multi_color_direction");
    if (mcd) { f->has_multi_color_direction = 1; safe_copy_str(f->multi_color_direction, sizeof f->multi_color_direction, mcd); }
    cJSON *extid = cJSON_GetObjectItemCaseSensitive(o, "external_id");
    if (extid) { f->has_external_id = 1; safe_copy_str(f->external_id, sizeof f->external_id, extid); }
    load_extra_raw(o, f->extra, sizeof f->extra);
    return 0;
}

static int load_spool_raw(const cJSON *o, Spool *s) {
    memset(s, 0, sizeof *s);
    if (load_id_raw(o, &s->id) != 0) return -1;
    safe_copy_str(s->registered, sizeof s->registered, cJSON_GetObjectItemCaseSensitive(o, "registered"));
    cJSON *fu = cJSON_GetObjectItemCaseSensitive(o, "first_used");
    if (fu) { s->has_first_used = 1; safe_copy_str(s->first_used, sizeof s->first_used, fu); }
    cJSON *lu = cJSON_GetObjectItemCaseSensitive(o, "last_used");
    if (lu) { s->has_last_used = 1; safe_copy_str(s->last_used, sizeof s->last_used, lu); }
    cJSON *fid = cJSON_GetObjectItemCaseSensitive(o, "filament_id");
    s->filament_id = cJSON_IsNumber(fid) ? fid->valueint : 0;
    cJSON *price = cJSON_GetObjectItemCaseSensitive(o, "price");
    if (cJSON_IsNumber(price)) { s->has_price = 1; s->price = price->valuedouble; }
    cJSON *iw = cJSON_GetObjectItemCaseSensitive(o, "initial_weight");
    if (cJSON_IsNumber(iw)) { s->has_initial_weight = 1; s->initial_weight = iw->valuedouble; }
    cJSON *sw = cJSON_GetObjectItemCaseSensitive(o, "spool_weight");
    if (cJSON_IsNumber(sw)) { s->has_spool_weight = 1; s->spool_weight = sw->valuedouble; }
    cJSON *uw = cJSON_GetObjectItemCaseSensitive(o, "used_weight");
    s->used_weight = cJSON_IsNumber(uw) ? uw->valuedouble : 0;
    cJSON *loc = cJSON_GetObjectItemCaseSensitive(o, "location");
    if (loc) { s->has_location = 1; safe_copy_str(s->location, sizeof s->location, loc); }
    cJSON *lot = cJSON_GetObjectItemCaseSensitive(o, "lot_nr");
    if (lot) { s->has_lot_nr = 1; safe_copy_str(s->lot_nr, sizeof s->lot_nr, lot); }
    cJSON *cm = cJSON_GetObjectItemCaseSensitive(o, "comment");
    if (cm) { s->has_comment = 1; safe_copy_str(s->comment, sizeof s->comment, cm); }
    cJSON *arc = cJSON_GetObjectItemCaseSensitive(o, "archived");
    s->archived = cJSON_IsTrue(arc) ? 1 : 0;
    load_extra_raw(o, s->extra, sizeof s->extra);
    return 0;
}

/* Атомарная запись: временный файл рядом + fsync + rename. */
static int write_file_atomic(const char *path, const char *data, size_t len) {
    char tmp[1024];
    int n = snprintf(tmp, sizeof tmp, "%s.tmp-%d", path, (int)getpid());
    if (n < 0 || (size_t)n >= sizeof tmp) return -1;
    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;
    size_t off = 0;
    while (off < len) {
        ssize_t w = write(fd, data + off, len - off);
        if (w < 0) { if (errno == EINTR) continue; close(fd); unlink(tmp); return -1; }
        off += (size_t)w;
    }
    if (fsync(fd) != 0) { close(fd); unlink(tmp); return -1; }
    if (close(fd) != 0) { unlink(tmp); return -1; }
    if (rename(tmp, path) != 0) { unlink(tmp); return -1; }
    return 0;
}

static void save_data(void) {
    cJSON *root = cJSON_CreateObject();
    cJSON_AddNumberToObject(root, "version", 1);
    cJSON *nid = cJSON_CreateObject();
    cJSON_AddNumberToObject(nid, "vendor", g_next_vendor_id);
    cJSON_AddNumberToObject(nid, "filament", g_next_filament_id);
    cJSON_AddNumberToObject(nid, "spool", g_next_spool_id);
    cJSON_AddItemToObject(root, "next_id", nid);

    cJSON *vs = cJSON_CreateArray();
    for (int i = 0; i < MAX_VENDORS; i++) if (g_vendors[i].id) cJSON_AddItemToArray(vs, vendor_raw_json(&g_vendors[i]));
    cJSON_AddItemToObject(root, "vendors", vs);

    cJSON *fs = cJSON_CreateArray();
    for (int i = 0; i < MAX_FILAMENTS; i++) if (g_filaments[i].id) cJSON_AddItemToArray(fs, filament_raw_json(&g_filaments[i]));
    cJSON_AddItemToObject(root, "filaments", fs);

    cJSON *sp = cJSON_CreateArray();
    for (int i = 0; i < MAX_SPOOLS; i++) if (g_spools[i].id) cJSON_AddItemToArray(sp, spool_raw_json(&g_spools[i]));
    cJSON_AddItemToObject(root, "spools", sp);

    char *text = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    if (!text) { fprintf(stderr, "microspool: ошибка сериализации данных для сохранения\n"); return; }
    if (write_file_atomic(g_data_path, text, strlen(text)) != 0) {
        fprintf(stderr, "microspool: не удалось сохранить %s: %s\n", g_data_path, strerror(errno));
    } else {
        log_v("сохранено %s (%zu байт)", g_data_path, strlen(text));
    }
    free(text);
    g_data_dirty_immediate = 0;
    g_use_pending = 0;
    g_last_use_flush_mono = mono_now();
}

/* next_id from the data file; out-of-range values fall back to 1 (the max-id
   scan below then moves it past the existing records). */
static int next_id_raw(const cJSON *n) {
    if (!cJSON_IsNumber(n)) return 1;
    double d = n->valuedouble;
    if (!(d >= 1 && d <= (double)INT_MAX) || d != floor(d)) return 1;
    return (int)d;
}

/* читает файл целиком; возвращает malloc'нутый буфер (с NUL) или NULL */
static char *read_file_all(const char *path, size_t *out_len) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return NULL; }
    long sz = ftell(f);
    if (sz < 0) { fclose(f); return NULL; }
    if (fseek(f, 0, SEEK_SET) != 0) { fclose(f); return NULL; }
    char *buf = malloc((size_t)sz + 1);
    if (!buf) { fclose(f); return NULL; }
    size_t rd = fread(buf, 1, (size_t)sz, f);
    fclose(f);
    buf[rd] = 0;
    if (out_len) *out_len = rd;
    return buf;
}

/* Загружает данные с диска. Возвращает 0 при успехе (в т.ч. если файла нет),
   -1 если файл есть, но повреждён/некорректной структуры (не трогаем файл). */
static int load_data(void) {
    struct stat st;
    if (stat(g_data_path, &st) != 0) {
        log_v("файл данных %s не найден, стартуем с пустой базой", g_data_path);
        return 0;
    }
    size_t len = 0;
    char *text = read_file_all(g_data_path, &len);
    if (!text) {
        fprintf(stderr, "microspool: не удалось прочитать %s: %s\n", g_data_path, strerror(errno));
        return -1;
    }
    cJSON *root = cJSON_Parse(text);
    free(text);
    if (!root || !cJSON_IsObject(root)) {
        fprintf(stderr, "microspool: файл данных %s повреждён (невалидный JSON)\n", g_data_path);
        if (root) cJSON_Delete(root);
        return -1;
    }
    cJSON *ver = cJSON_GetObjectItemCaseSensitive(root, "version");
    if (!cJSON_IsNumber(ver) || ver->valueint != 1) {
        fprintf(stderr, "microspool: файл данных %s имеет неизвестную версию формата\n", g_data_path);
        cJSON_Delete(root);
        return -1;
    }
    cJSON *nid = cJSON_GetObjectItemCaseSensitive(root, "next_id");
    cJSON *vs = cJSON_GetObjectItemCaseSensitive(root, "vendors");
    cJSON *fs = cJSON_GetObjectItemCaseSensitive(root, "filaments");
    cJSON *sp = cJSON_GetObjectItemCaseSensitive(root, "spools");
    if (!cJSON_IsObject(nid) || !cJSON_IsArray(vs) || !cJSON_IsArray(fs) || !cJSON_IsArray(sp)) {
        fprintf(stderr, "microspool: файл данных %s повреждён (нет ожидаемых полей)\n", g_data_path);
        cJSON_Delete(root);
        return -1;
    }
    memset(g_vendors, 0, sizeof g_vendors);
    memset(g_filaments, 0, sizeof g_filaments);
    memset(g_spools, 0, sizeof g_spools);

    int i = 0;
    cJSON *item;
    cJSON_ArrayForEach(item, vs) {
        if (i >= MAX_VENDORS) { fprintf(stderr, "microspool: слишком много вендоров в файле данных\n"); cJSON_Delete(root); return -1; }
        if (load_vendor_raw(item, &g_vendors[i]) != 0) { fprintf(stderr, "microspool: повреждена запись вендора #%d\n", i); cJSON_Delete(root); return -1; }
        i++;
    }
    i = 0;
    cJSON_ArrayForEach(item, fs) {
        if (i >= MAX_FILAMENTS) { fprintf(stderr, "microspool: слишком много филаментов в файле данных\n"); cJSON_Delete(root); return -1; }
        if (load_filament_raw(item, &g_filaments[i]) != 0) { fprintf(stderr, "microspool: повреждена запись филамента #%d\n", i); cJSON_Delete(root); return -1; }
        i++;
    }
    i = 0;
    cJSON_ArrayForEach(item, sp) {
        if (i >= MAX_SPOOLS) { fprintf(stderr, "microspool: слишком много катушек в файле данных\n"); cJSON_Delete(root); return -1; }
        if (load_spool_raw(item, &g_spools[i]) != 0) { fprintf(stderr, "microspool: повреждена запись катушки #%d\n", i); cJSON_Delete(root); return -1; }
        i++;
    }

    cJSON *nv = cJSON_GetObjectItemCaseSensitive(nid, "vendor");
    cJSON *nf = cJSON_GetObjectItemCaseSensitive(nid, "filament");
    cJSON *ns = cJSON_GetObjectItemCaseSensitive(nid, "spool");
    /* duplicate ids would make find/patch/delete hit only the first record */
    for (int a = 0; a < MAX_VENDORS; a++) for (int b = a + 1; b < MAX_VENDORS && g_vendors[a].id; b++)
        if (g_vendors[a].id == g_vendors[b].id) { fprintf(stderr, "microspool: duplicate vendor id %d in the data file\n", g_vendors[a].id); cJSON_Delete(root); return -1; }
    for (int a = 0; a < MAX_FILAMENTS; a++) for (int b = a + 1; b < MAX_FILAMENTS && g_filaments[a].id; b++)
        if (g_filaments[a].id == g_filaments[b].id) { fprintf(stderr, "microspool: duplicate filament id %d in the data file\n", g_filaments[a].id); cJSON_Delete(root); return -1; }
    for (int a = 0; a < MAX_SPOOLS; a++) for (int b = a + 1; b < MAX_SPOOLS && g_spools[a].id; b++)
        if (g_spools[a].id == g_spools[b].id) { fprintf(stderr, "microspool: duplicate spool id %d in the data file\n", g_spools[a].id); cJSON_Delete(root); return -1; }

    g_next_vendor_id = next_id_raw(nv);
    g_next_filament_id = next_id_raw(nf);
    g_next_spool_id = next_id_raw(ns);
    /* защита от рассинхронизации: next_id не должен быть <= максимального id */
    for (int k = 0; k < MAX_VENDORS; k++) if (g_vendors[k].id >= g_next_vendor_id) g_next_vendor_id = g_vendors[k].id + 1;
    for (int k = 0; k < MAX_FILAMENTS; k++) if (g_filaments[k].id >= g_next_filament_id) g_next_filament_id = g_filaments[k].id + 1;
    for (int k = 0; k < MAX_SPOOLS; k++) if (g_spools[k].id >= g_next_spool_id) g_next_spool_id = g_spools[k].id + 1;

    cJSON_Delete(root);
    log_v("загружено %s", g_data_path);
    return 0;
}

/* Рассылка события всем WebSocket-клиентам; определена в сетевой секции ниже,
   объявлена здесь, т.к. используется в обработчиках сущностей. Забирает
   владение payload_owned (удаляет его сама). */
static void ws_broadcast_event(const char *type, const char *resource, cJSON *payload_owned);

/* ------------------------------------------------------------------------ */
/* Сборка объектов ответа (с вложенностью)                                  */
/* ------------------------------------------------------------------------ */

static cJSON *vendor_json(const Vendor *v) { return vendor_raw_json(v); }

static cJSON *filament_response_json(const Filament *f) {
    cJSON *o = cJSON_CreateObject();
    cJSON_AddNumberToObject(o, "id", f->id);
    cJSON_AddStringToObject(o, "registered", f->registered);
    if (f->has_name) cJSON_AddStringToObject(o, "name", f->name);
    if (f->has_vendor_id) {
        Vendor *vv = vendor_find(f->vendor_id);
        if (vv) cJSON_AddItemToObject(o, "vendor", vendor_json(vv));
    }
    if (f->has_material) cJSON_AddStringToObject(o, "material", f->material);
    if (f->has_price) cJSON_AddNumberToObject(o, "price", f->price);
    cJSON_AddNumberToObject(o, "density", f->density);
    cJSON_AddNumberToObject(o, "diameter", f->diameter);
    if (f->has_weight) cJSON_AddNumberToObject(o, "weight", f->weight);
    if (f->has_spool_weight) cJSON_AddNumberToObject(o, "spool_weight", f->spool_weight);
    if (f->has_article_number) cJSON_AddStringToObject(o, "article_number", f->article_number);
    if (f->has_comment) cJSON_AddStringToObject(o, "comment", f->comment);
    if (f->has_settings_extruder_temp) cJSON_AddNumberToObject(o, "settings_extruder_temp", f->settings_extruder_temp);
    if (f->has_settings_bed_temp) cJSON_AddNumberToObject(o, "settings_bed_temp", f->settings_bed_temp);
    if (f->has_color_hex) cJSON_AddStringToObject(o, "color_hex", f->color_hex);
    if (f->has_multi_color_hexes) cJSON_AddStringToObject(o, "multi_color_hexes", f->multi_color_hexes);
    if (f->has_multi_color_direction) cJSON_AddStringToObject(o, "multi_color_direction", f->multi_color_direction);
    if (f->has_external_id) cJSON_AddStringToObject(o, "external_id", f->external_id);
    cJSON *extra = cJSON_Parse(f->extra[0] ? f->extra : "{}");
    cJSON_AddItemToObject(o, "extra", extra ? extra : cJSON_CreateObject());
    return o;
}

/* weight_g -> длина в мм; денаминатор всегда > 0, т.к. density/diameter обязательны и > 0 */
static double weight_to_length_mm(double weight_g, double density, double diameter_mm) {
    double r = diameter_mm / 2.0;
    double denom = density * M_PI * r * r / 1000.0;
    if (denom <= 0) return 0;
    double len = weight_g / denom;
    return isfinite(len) ? len : 0;   /* cJSON would print inf as null */
}

static cJSON *spool_response_json(const Spool *s) {
    cJSON *o = cJSON_CreateObject();
    cJSON_AddNumberToObject(o, "id", s->id);
    cJSON_AddStringToObject(o, "registered", s->registered);
    if (s->has_first_used) cJSON_AddStringToObject(o, "first_used", s->first_used);
    if (s->has_last_used) cJSON_AddStringToObject(o, "last_used", s->last_used);
    Filament *fil = filament_find(s->filament_id);
    cJSON_AddItemToObject(o, "filament", fil ? filament_response_json(fil) : cJSON_CreateNull());
    if (s->has_price) cJSON_AddNumberToObject(o, "price", s->price);

    double remaining = 0; int has_remaining = 0;
    if (s->has_initial_weight) {
        remaining = s->initial_weight - s->used_weight;
        if (remaining < 0) remaining = 0;
        has_remaining = 1;
        cJSON_AddNumberToObject(o, "remaining_weight", remaining);
    }
    if (s->has_initial_weight) cJSON_AddNumberToObject(o, "initial_weight", s->initial_weight);
    if (s->has_spool_weight) cJSON_AddNumberToObject(o, "spool_weight", s->spool_weight);
    cJSON_AddNumberToObject(o, "used_weight", s->used_weight);

    if (fil) {
        double used_length = weight_to_length_mm(s->used_weight, fil->density, fil->diameter);
        cJSON_AddNumberToObject(o, "used_length", used_length);
        if (has_remaining) {
            double remaining_length = weight_to_length_mm(remaining, fil->density, fil->diameter);
            cJSON_AddNumberToObject(o, "remaining_length", remaining_length);
        }
    } else {
        cJSON_AddNumberToObject(o, "used_length", 0);
    }

    if (s->has_location) cJSON_AddStringToObject(o, "location", s->location);
    if (s->has_lot_nr) cJSON_AddStringToObject(o, "lot_nr", s->lot_nr);
    if (s->has_comment) cJSON_AddStringToObject(o, "comment", s->comment);
    cJSON_AddBoolToObject(o, "archived", s->archived);
    cJSON *extra = cJSON_Parse(s->extra[0] ? s->extra : "{}");
    cJSON_AddItemToObject(o, "extra", extra ? extra : cJSON_CreateObject());
    return o;
}

/* ------------------------------------------------------------------------ */
/* Обработчики сущностей.                                                   */
/* Соглашение: возврат 0 = успех (body_out заполнен), иначе — код HTTP-     */
/* ошибки (400/404/...), msg заполнен человекочитаемым текстом.            */
/* ------------------------------------------------------------------------ */

#define VENDOR_ACTIVE_COUNT(n) do { n = 0; for (int _i = 0; _i < MAX_VENDORS; _i++) if (g_vendors[_i].id) n++; } while (0)
#define FILAMENT_ACTIVE_COUNT(n) do { n = 0; for (int _i = 0; _i < MAX_FILAMENTS; _i++) if (g_filaments[_i].id) n++; } while (0)
#define SPOOL_ACTIVE_COUNT(n) do { n = 0; for (int _i = 0; _i < MAX_SPOOLS; _i++) if (g_spools[_i].id) n++; } while (0)

/* ---- Vendor ---- */

static int vendor_apply_fields(const cJSON *input, Vendor *v, int is_create, char *msg, size_t msgsz) {
    int present, has, rc;
    char buf[STR_CAP];

    rc = jget_string(input, "name", buf, sizeof buf, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "name must be a string up to 256 chars"); return -1; }
    if (is_create) {
        if (!present || !has || buf[0] == 0) { snprintf(msg, msgsz, "name is required"); return -1; }
        strcpy(v->name, buf);
    } else if (present) {
        if (!has || buf[0] == 0) { snprintf(msg, msgsz, "name cannot be empty"); return -1; }
        strcpy(v->name, buf);
    }

    rc = jget_string(input, "comment", buf, sizeof buf, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "comment must be a string up to 256 chars"); return -1; }
    if (present) { v->has_comment = has; if (has) strcpy(v->comment, buf); }

    double num;
    rc = jget_number(input, "empty_spool_weight", &num, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "empty_spool_weight must be a number"); return -1; }
    if (present) { v->has_empty_spool_weight = has; if (has) v->empty_spool_weight = num; }

    rc = jget_string(input, "external_id", buf, sizeof buf, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "external_id must be a string up to 256 chars"); return -1; }
    if (present) { v->has_external_id = has; if (has) strcpy(v->external_id, buf); }

    char ebuf[EXTRA_CAP];
    rc = jget_extra(input, ebuf, sizeof ebuf, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "extra must be an object with string values (serialized <=%d bytes)", EXTRA_CAP - 1); return -1; }
    if (present) strcpy(v->extra, has ? ebuf : "{}");
    else if (is_create) strcpy(v->extra, "{}");
    return 0;
}

static int vendor_create(const cJSON *input, cJSON **body_out, char *msg, size_t msgsz) {
    int n; VENDOR_ACTIVE_COUNT(n);
    if (n >= MAX_VENDORS) { snprintf(msg, msgsz, "vendor limit reached (%d)", MAX_VENDORS); return 400; }
    Vendor *slot = vendor_free_slot();
    if (!slot) { snprintf(msg, msgsz, "vendor limit reached (%d)", MAX_VENDORS); return 400; }
    Vendor tmp; memset(&tmp, 0, sizeof tmp);
    if (vendor_apply_fields(input, &tmp, 1, msg, msgsz) != 0) return 400;
    if (g_next_vendor_id >= INT_MAX) { snprintf(msg, msgsz, "vendor ids exhausted"); return 400; }
    tmp.id = g_next_vendor_id++;
    now_iso(tmp.registered, sizeof tmp.registered);
    *slot = tmp;
    *body_out = vendor_json(slot);
    return 0;
}

static int vendor_get(int id, cJSON **body_out, char *msg, size_t msgsz) {
    Vendor *v = vendor_find(id);
    if (!v) { snprintf(msg, msgsz, "vendor %d not found", id); return 404; }
    *body_out = vendor_json(v);
    return 0;
}

static int vendor_list(cJSON **body_out, int *count) {
    int ids[MAX_VENDORS], n = 0;
    for (int i = 0; i < MAX_VENDORS; i++) if (g_vendors[i].id) ids[n++] = g_vendors[i].id;
    qsort(ids, n, sizeof(int), cmp_int);
    cJSON *arr = cJSON_CreateArray();
    for (int i = 0; i < n; i++) cJSON_AddItemToArray(arr, vendor_json(vendor_find(ids[i])));
    *body_out = arr;
    *count = n;
    return 0;
}

static int vendor_patch(int id, const cJSON *input, cJSON **body_out, char *msg, size_t msgsz) {
    Vendor *v = vendor_find(id);
    if (!v) { snprintf(msg, msgsz, "vendor %d not found", id); return 404; }
    Vendor tmp = *v;
    if (vendor_apply_fields(input, &tmp, 0, msg, msgsz) != 0) return 400;
    *v = tmp;
    *body_out = vendor_json(v);
    return 0;
}

static int vendor_delete(int id, cJSON **body_out, char *msg, size_t msgsz) {
    Vendor *v = vendor_find(id);
    if (!v) { snprintf(msg, msgsz, "vendor %d not found", id); return 404; }
    cJSON *deleted = vendor_json(v);
    for (int i = 0; i < MAX_FILAMENTS; i++) {
        if (g_filaments[i].id && g_filaments[i].has_vendor_id && g_filaments[i].vendor_id == id) {
            g_filaments[i].has_vendor_id = 0;
            g_filaments[i].vendor_id = 0;
        }
    }
    memset(v, 0, sizeof *v);
    *body_out = deleted;
    return 0;
}

/* ---- Filament ---- */

static int is_hex6(const char *s) {
    if (strlen(s) != 6) return 0;
    for (int i = 0; i < 6; i++) if (!isxdigit((unsigned char)s[i])) return 0;
    return 1;
}

static int filament_apply_fields(const cJSON *input, Filament *f, int is_create, char *msg, size_t msgsz) {
    int present, has, rc;
    char buf[STR_CAP];

    rc = jget_string(input, "name", buf, sizeof buf, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "name must be a string up to 256 chars"); return -1; }
    if (present) { f->has_name = has; if (has) strcpy(f->name, buf); }

    int vid;
    rc = jget_int(input, "vendor_id", &vid, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "vendor_id must be an integer"); return -1; }
    if (present) {
        if (has) {
            if (!vendor_find(vid)) { snprintf(msg, msgsz, "vendor_id references nonexistent vendor %d", vid); return -1; }
            f->has_vendor_id = 1; f->vendor_id = vid;
        } else { f->has_vendor_id = 0; f->vendor_id = 0; }
    }

    rc = jget_string(input, "material", buf, sizeof buf, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "material must be a string up to 256 chars"); return -1; }
    if (present) { f->has_material = has; if (has) strcpy(f->material, buf); }

    double num;
    rc = jget_number(input, "price", &num, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "price must be a number"); return -1; }
    if (present) { f->has_price = has; if (has) f->price = num; }

    rc = jget_number(input, "density", &num, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "density must be a number"); return -1; }
    if (is_create) {
        if (!present || !has) { snprintf(msg, msgsz, "density is required"); return -1; }
        if (num <= 0) { snprintf(msg, msgsz, "density must be > 0"); return -1; }
        f->density = num;
    } else if (present) {
        if (!has) { snprintf(msg, msgsz, "density cannot be null"); return -1; }
        if (num <= 0) { snprintf(msg, msgsz, "density must be > 0"); return -1; }
        f->density = num;
    }

    rc = jget_number(input, "diameter", &num, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "diameter must be a number"); return -1; }
    if (is_create) {
        if (!present || !has) { snprintf(msg, msgsz, "diameter is required"); return -1; }
        if (num <= 0) { snprintf(msg, msgsz, "diameter must be > 0"); return -1; }
        f->diameter = num;
    } else if (present) {
        if (!has) { snprintf(msg, msgsz, "diameter cannot be null"); return -1; }
        if (num <= 0) { snprintf(msg, msgsz, "diameter must be > 0"); return -1; }
        f->diameter = num;
    }

    rc = jget_number(input, "weight", &num, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "weight must be a number"); return -1; }
    if (present) { f->has_weight = has; if (has) f->weight = num; }

    rc = jget_number(input, "spool_weight", &num, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "spool_weight must be a number"); return -1; }
    if (present) { f->has_spool_weight = has; if (has) f->spool_weight = num; }

    rc = jget_string(input, "article_number", buf, sizeof buf, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "article_number must be a string up to 256 chars"); return -1; }
    if (present) { f->has_article_number = has; if (has) strcpy(f->article_number, buf); }

    rc = jget_string(input, "comment", buf, sizeof buf, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "comment must be a string up to 256 chars"); return -1; }
    if (present) { f->has_comment = has; if (has) strcpy(f->comment, buf); }

    int inum;
    rc = jget_int(input, "settings_extruder_temp", &inum, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "settings_extruder_temp must be an integer"); return -1; }
    if (present) { f->has_settings_extruder_temp = has; if (has) f->settings_extruder_temp = inum; }

    rc = jget_int(input, "settings_bed_temp", &inum, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "settings_bed_temp must be an integer"); return -1; }
    if (present) { f->has_settings_bed_temp = has; if (has) f->settings_bed_temp = inum; }

    rc = jget_string(input, "color_hex", buf, sizeof f->color_hex, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "color_hex must be a 6-char hex string"); return -1; }
    if (present) {
        if (has) {
            if (!is_hex6(buf)) { snprintf(msg, msgsz, "color_hex must be 6 hex chars without '#'"); return -1; }
            f->has_color_hex = 1; strcpy(f->color_hex, buf);
        } else { f->has_color_hex = 0; f->color_hex[0] = 0; }
    }

    rc = jget_string(input, "multi_color_hexes", buf, sizeof buf, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "multi_color_hexes must be a string up to 256 chars"); return -1; }
    if (present) { f->has_multi_color_hexes = has; if (has) strcpy(f->multi_color_hexes, buf); }

    rc = jget_string(input, "multi_color_direction", buf, sizeof f->multi_color_direction, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "multi_color_direction must be a short string (<=31 chars)"); return -1; }
    if (present) { f->has_multi_color_direction = has; if (has) strcpy(f->multi_color_direction, buf); }

    rc = jget_string(input, "external_id", buf, sizeof buf, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "external_id must be a string up to 256 chars"); return -1; }
    if (present) { f->has_external_id = has; if (has) strcpy(f->external_id, buf); }

    char ebuf[EXTRA_CAP];
    rc = jget_extra(input, ebuf, sizeof ebuf, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "extra must be an object with string values (serialized <=%d bytes)", EXTRA_CAP - 1); return -1; }
    if (present) strcpy(f->extra, has ? ebuf : "{}");
    else if (is_create) strcpy(f->extra, "{}");
    return 0;
}

static int filament_create(const cJSON *input, cJSON **body_out, char *msg, size_t msgsz) {
    int n; FILAMENT_ACTIVE_COUNT(n);
    if (n >= MAX_FILAMENTS) { snprintf(msg, msgsz, "filament limit reached (%d)", MAX_FILAMENTS); return 400; }
    Filament *slot = filament_free_slot();
    if (!slot) { snprintf(msg, msgsz, "filament limit reached (%d)", MAX_FILAMENTS); return 400; }
    Filament tmp; memset(&tmp, 0, sizeof tmp);
    if (filament_apply_fields(input, &tmp, 1, msg, msgsz) != 0) return 400;
    if (g_next_filament_id >= INT_MAX) { snprintf(msg, msgsz, "filament ids exhausted"); return 400; }
    tmp.id = g_next_filament_id++;
    now_iso(tmp.registered, sizeof tmp.registered);
    *slot = tmp;
    *body_out = filament_response_json(slot);
    return 0;
}

static int filament_get(int id, cJSON **body_out, char *msg, size_t msgsz) {
    Filament *f = filament_find(id);
    if (!f) { snprintf(msg, msgsz, "filament %d not found", id); return 404; }
    *body_out = filament_response_json(f);
    return 0;
}

static int filament_list(cJSON **body_out, int *count) {
    int ids[MAX_FILAMENTS], n = 0;
    for (int i = 0; i < MAX_FILAMENTS; i++) if (g_filaments[i].id) ids[n++] = g_filaments[i].id;
    qsort(ids, n, sizeof(int), cmp_int);
    cJSON *arr = cJSON_CreateArray();
    for (int i = 0; i < n; i++) cJSON_AddItemToArray(arr, filament_response_json(filament_find(ids[i])));
    *body_out = arr;
    *count = n;
    return 0;
}

static int filament_patch(int id, const cJSON *input, cJSON **body_out, char *msg, size_t msgsz) {
    Filament *f = filament_find(id);
    if (!f) { snprintf(msg, msgsz, "filament %d not found", id); return 404; }
    Filament tmp = *f;
    if (filament_apply_fields(input, &tmp, 0, msg, msgsz) != 0) return 400;
    *f = tmp;
    *body_out = filament_response_json(f);
    return 0;
}

static int filament_delete(int id, cJSON **body_out, char *msg, size_t msgsz) {
    Filament *f = filament_find(id);
    if (!f) { snprintf(msg, msgsz, "filament %d not found", id); return 404; }
    for (int i = 0; i < MAX_SPOOLS; i++) {
        if (g_spools[i].id && g_spools[i].filament_id == id) {
            snprintf(msg, msgsz, "filament %d is referenced by existing spools", id);
            return 400;
        }
    }
    cJSON *deleted = filament_response_json(f);
    memset(f, 0, sizeof *f);
    *body_out = deleted;
    return 0;
}

/* ---- Spool ---- */

/* remaining_weight / used_weight из входа, применяются после того, как
   известен итоговый initial_weight записи. */
static int spool_apply_weight_delta_input(const cJSON *input, Spool *s, char *msg, size_t msgsz) {
    int present_r, has_r, present_u, has_u, rc;
    double remaining_val = 0, used_val = 0;
    rc = jget_number(input, "remaining_weight", &remaining_val, &present_r, &has_r);
    if (rc != 0) { snprintf(msg, msgsz, "remaining_weight must be a number"); return -1; }
    rc = jget_number(input, "used_weight", &used_val, &present_u, &has_u);
    if (rc != 0) { snprintf(msg, msgsz, "used_weight must be a number"); return -1; }
    if (present_r && present_u) {
        snprintf(msg, msgsz, "cannot set both remaining_weight and used_weight");
        return -1;
    }
    if (present_r) {
        if (has_r) {
            if (!s->has_initial_weight) {
                snprintf(msg, msgsz, "remaining_weight requires a known initial_weight");
                return -1;
            }
            double uw = s->initial_weight - remaining_val;
            if (uw < 0) uw = 0;
            s->used_weight = uw;
        } else {
            s->used_weight = 0;
        }
    } else if (present_u) {
        s->used_weight = has_u ? used_val : 0;
    }
    return 0;
}

static int spool_apply_fields(const cJSON *input, Spool *s, int is_create, char *msg, size_t msgsz) {
    int present, has, rc;
    char buf[STR_CAP];
    char tsbuf[32];

    int fid;
    rc = jget_int(input, "filament_id", &fid, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "filament_id must be an integer"); return -1; }
    if (is_create) {
        if (!present || !has) { snprintf(msg, msgsz, "filament_id is required"); return -1; }
        if (!filament_find(fid)) { snprintf(msg, msgsz, "filament_id references nonexistent filament %d", fid); return -1; }
        s->filament_id = fid;
    } else if (present) {
        if (!has) { snprintf(msg, msgsz, "filament_id cannot be null"); return -1; }
        if (!filament_find(fid)) { snprintf(msg, msgsz, "filament_id references nonexistent filament %d", fid); return -1; }
        s->filament_id = fid;
    }

    double num;
    rc = jget_number(input, "initial_weight", &num, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "initial_weight must be a number"); return -1; }
    if (present) { s->has_initial_weight = has; if (has) s->initial_weight = num; }
    else if (is_create) {
        Filament *fil = filament_find(s->filament_id);
        if (fil && fil->has_weight) { s->has_initial_weight = 1; s->initial_weight = fil->weight; }
    }

    rc = jget_number(input, "spool_weight", &num, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "spool_weight must be a number"); return -1; }
    if (present) { s->has_spool_weight = has; if (has) s->spool_weight = num; }

    rc = jget_number(input, "price", &num, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "price must be a number"); return -1; }
    if (present) { s->has_price = has; if (has) s->price = num; }

    rc = jget_string(input, "first_used", tsbuf, sizeof tsbuf, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "first_used must be a short ISO-8601 string"); return -1; }
    if (present) { s->has_first_used = has; if (has) strcpy(s->first_used, tsbuf); }

    rc = jget_string(input, "last_used", tsbuf, sizeof tsbuf, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "last_used must be a short ISO-8601 string"); return -1; }
    if (present) { s->has_last_used = has; if (has) strcpy(s->last_used, tsbuf); }

    rc = jget_string(input, "location", buf, sizeof buf, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "location must be a string up to 256 chars"); return -1; }
    if (present) { s->has_location = has; if (has) strcpy(s->location, buf); }

    rc = jget_string(input, "lot_nr", buf, sizeof buf, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "lot_nr must be a string up to 256 chars"); return -1; }
    if (present) { s->has_lot_nr = has; if (has) strcpy(s->lot_nr, buf); }

    rc = jget_string(input, "comment", buf, sizeof buf, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "comment must be a string up to 256 chars"); return -1; }
    if (present) { s->has_comment = has; if (has) strcpy(s->comment, buf); }

    int bval;
    rc = jget_bool(input, "archived", &bval, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "archived must be a boolean"); return -1; }
    if (present) s->archived = has ? bval : 0;

    char ebuf[EXTRA_CAP];
    rc = jget_extra(input, ebuf, sizeof ebuf, &present, &has);
    if (rc != 0) { snprintf(msg, msgsz, "extra must be an object with string values (serialized <=%d bytes)", EXTRA_CAP - 1); return -1; }
    if (present) strcpy(s->extra, has ? ebuf : "{}");
    else if (is_create) strcpy(s->extra, "{}");

    if (spool_apply_weight_delta_input(input, s, msg, msgsz) != 0) return -1;
    return 0;
}

static int spool_create(const cJSON *input, cJSON **body_out, char *msg, size_t msgsz) {
    int n; SPOOL_ACTIVE_COUNT(n);
    if (n >= MAX_SPOOLS) { snprintf(msg, msgsz, "spool limit reached (%d)", MAX_SPOOLS); return 400; }
    Spool *slot = spool_free_slot();
    if (!slot) { snprintf(msg, msgsz, "spool limit reached (%d)", MAX_SPOOLS); return 400; }
    Spool tmp; memset(&tmp, 0, sizeof tmp);
    strcpy(tmp.extra, "{}");
    if (spool_apply_fields(input, &tmp, 1, msg, msgsz) != 0) return 400;
    if (g_next_spool_id >= INT_MAX) { snprintf(msg, msgsz, "spool ids exhausted"); return 400; }
    tmp.id = g_next_spool_id++;
    now_iso(tmp.registered, sizeof tmp.registered);
    *slot = tmp;
    *body_out = spool_response_json(slot);
    return 0;
}

static int spool_get(int id, cJSON **body_out, char *msg, size_t msgsz) {
    Spool *s = spool_find(id);
    if (!s) { snprintf(msg, msgsz, "spool %d not found", id); return 404; }
    *body_out = spool_response_json(s);
    return 0;
}

static int spool_list(int allow_archived, cJSON **body_out, int *count) {
    int ids[MAX_SPOOLS], n = 0;
    for (int i = 0; i < MAX_SPOOLS; i++) {
        if (!g_spools[i].id) continue;
        if (g_spools[i].archived && !allow_archived) continue;
        ids[n++] = g_spools[i].id;
    }
    qsort(ids, n, sizeof(int), cmp_int);
    cJSON *arr = cJSON_CreateArray();
    for (int i = 0; i < n; i++) cJSON_AddItemToArray(arr, spool_response_json(spool_find(ids[i])));
    *body_out = arr;
    *count = n;
    return 0;
}

static int spool_patch(int id, const cJSON *input, cJSON **body_out, char *msg, size_t msgsz) {
    Spool *s = spool_find(id);
    if (!s) { snprintf(msg, msgsz, "spool %d not found", id); return 404; }
    Spool tmp = *s;
    if (spool_apply_fields(input, &tmp, 0, msg, msgsz) != 0) return 400;
    *s = tmp;
    *body_out = spool_response_json(s);
    return 0;
}

static int spool_delete(int id, cJSON **body_out, char *msg, size_t msgsz) {
    Spool *s = spool_find(id);
    if (!s) { snprintf(msg, msgsz, "spool %d not found", id); return 404; }
    cJSON *deleted = spool_response_json(s);
    memset(s, 0, sizeof *s);
    *body_out = deleted;
    return 0;
}

static int spool_use(int id, const cJSON *input, cJSON **body_out, char *msg, size_t msgsz) {
    Spool *s = spool_find(id);
    if (!s) { snprintf(msg, msgsz, "spool %d not found", id); return 404; }
    int present_l, has_l, present_w, has_w, rc;
    double use_length = 0, use_weight = 0;
    rc = jget_number(input, "use_length", &use_length, &present_l, &has_l);
    if (rc != 0) { snprintf(msg, msgsz, "use_length must be a number"); return 400; }
    rc = jget_number(input, "use_weight", &use_weight, &present_w, &has_w);
    if (rc != 0) { snprintf(msg, msgsz, "use_weight must be a number"); return 400; }
    int have_l = present_l && has_l;
    int have_w = present_w && has_w;
    if (have_l == have_w) {
        snprintf(msg, msgsz, "exactly one of use_length or use_weight is required");
        return 400;
    }
    Filament *fil = filament_find(s->filament_id);
    double delta;
    if (have_w) {
        delta = use_weight;
    } else {
        double density = fil ? fil->density : 0, diameter = fil ? fil->diameter : 0;
        double r = diameter / 2.0;
        double denom = density * M_PI * r * r / 1000.0; /* г на мм длины */
        delta = use_length * denom;
    }
    if (!isfinite(s->used_weight + delta)) { snprintf(msg, msgsz, "use out of range"); return 400; }
    s->used_weight += delta;
    char ts[32];
    now_iso(ts, sizeof ts);
    if (!s->has_first_used) { s->has_first_used = 1; strcpy(s->first_used, ts); }
    s->has_last_used = 1; strcpy(s->last_used, ts);
    *body_out = spool_response_json(s);
    return 0;
}

/* ------------------------------------------------------------------------ */
/* Мелкие строковые утилиты (без strcasecmp/strcasestr — своя реализация,   */
/* чтобы не зависеть от расширений libc).                                   */
/* ------------------------------------------------------------------------ */

static int str_ieq(const char *a, const char *b) {
    while (*a && *b) {
        if (tolower((unsigned char)*a) != tolower((unsigned char)*b)) return 0;
        a++; b++;
    }
    return *a == 0 && *b == 0;
}

static int ci_contains(const char *hay, const char *needle) {
    size_t hn = strlen(hay), nn = strlen(needle);
    if (nn == 0 || nn > hn) return 0;
    for (size_t i = 0; i + nn <= hn; i++) {
        size_t j = 0;
        for (; j < nn; j++) if (tolower((unsigned char)hay[i+j]) != tolower((unsigned char)needle[j])) break;
        if (j == nn) return 1;
    }
    return 0;
}

/* Сортировка + дедупликация указателей на C-строки (использует existing STR_CAP
   буферы записей, ничего не копирует) — для /api/v1/material|location|lot-number. */
static int cmp_str_ptr(const void *a, const void *b) {
    return strcmp(*(const char * const *)a, *(const char * const *)b);
}

static cJSON *unique_sorted_strings(const char **items, int n) {
    if (n > 1) qsort(items, (size_t)n, sizeof(char *), cmp_str_ptr);
    cJSON *arr = cJSON_CreateArray();
    for (int i = 0; i < n; i++) {
        if (i > 0 && strcmp(items[i], items[i - 1]) == 0) continue;
        cJSON_AddItemToArray(arr, cJSON_CreateString(items[i]));
    }
    return arr;
}

/* ------------------------------------------------------------------------ */
/* HTTP: разбор запроса                                                     */
/* ------------------------------------------------------------------------ */

typedef struct {
    char method[8];
    char path[1024];      /* сырой путь вместе с query string */
    long content_length;
    int  has_content_length;
    int  chunked;
    int  is_ws_upgrade;
    char ws_key[192];
    int  accepts_gzip;    /* Accept-Encoding содержит "gzip" (для GET /, /index.html) */
    int  has_host;     char host[256];
    int  has_origin;   char origin[256];
    char content_type[128];
} HttpRequestMeta;

static const char *find_header_end(const char *buf, size_t len) {
    if (len < 4) return NULL;
    for (size_t i = 0; i + 4 <= len; i++) {
        if (buf[i] == '\r' && buf[i+1] == '\n' && buf[i+2] == '\r' && buf[i+3] == '\n') return buf + i;
    }
    return NULL;
}

/* buf/hdr_len — только текст заголовков, БЕЗ завершающего \r\n\r\n. */
static int parse_http_request_line_and_headers(const char *buf, size_t hdr_len, HttpRequestMeta *m) {
    memset(m, 0, sizeof *m);
    if (hdr_len > HEADER_LIMIT) return -1;
    char tmp[HEADER_LIMIT + 1];
    memcpy(tmp, buf, hdr_len);
    tmp[hdr_len] = 0;

    char *saveptr = NULL;
    char *line = strtok_r(tmp, "\r\n", &saveptr);
    if (!line) return -1;

    char *sp1 = strchr(line, ' ');
    if (!sp1) return -1;
    size_t mlen = (size_t)(sp1 - line);
    if (mlen == 0 || mlen >= sizeof m->method) return -1;
    memcpy(m->method, line, mlen); m->method[mlen] = 0;

    char *p = sp1 + 1;
    char *sp2 = strchr(p, ' ');
    if (!sp2) return -1;
    size_t plen = (size_t)(sp2 - p);
    if (plen == 0 || plen >= sizeof m->path) return -1;
    memcpy(m->path, p, plen); m->path[plen] = 0;

    int upgrade_hdr = 0;
    while ((line = strtok_r(NULL, "\r\n", &saveptr)) != NULL) {
        char *colon = strchr(line, ':');
        if (!colon) continue;
        *colon = 0;
        char *key = line;
        char *val = colon + 1;
        while (*val == ' ' || *val == '\t') val++;
        for (char *e = val + strlen(val); e > val && (e[-1] == ' ' || e[-1] == '\t'); ) *--e = 0;

        if (str_ieq(key, "Content-Length")) {
            m->has_content_length = 1;
            m->content_length = atol(val);
        } else if (str_ieq(key, "Transfer-Encoding")) {
            if (ci_contains(val, "chunked")) m->chunked = 1;
        } else if (str_ieq(key, "Upgrade")) {
            if (ci_contains(val, "websocket")) upgrade_hdr = 1;
        } else if (str_ieq(key, "Accept-Encoding")) {
            if (ci_contains(val, "gzip")) m->accepts_gzip = 1;
        } else if (str_ieq(key, "Sec-WebSocket-Key")) {
            strncpy(m->ws_key, val, sizeof m->ws_key - 1);
        } else if (str_ieq(key, "Host")) {
            if (m->has_host || strlen(val) >= sizeof m->host) return -1;   /* duplicate or oversized */
            m->has_host = 1;
            memcpy(m->host, val, strlen(val) + 1);
        } else if (str_ieq(key, "Origin")) {
            if (m->has_origin || strlen(val) >= sizeof m->origin) return -1;
            m->has_origin = 1;
            memcpy(m->origin, val, strlen(val) + 1);
        } else if (str_ieq(key, "Content-Type")) {
            strncpy(m->content_type, val, sizeof m->content_type - 1);
        }
    }
    m->is_ws_upgrade = upgrade_hdr;
    return 0;
}

static void split_path(const char *raw, char *out_path, size_t out_sz) {
    const char *q = strchr(raw, '?');
    size_t len = q ? (size_t)(q - raw) : strlen(raw);
    if (len >= out_sz) len = out_sz - 1;
    memcpy(out_path, raw, len);
    out_path[len] = 0;
}

static int query_allow_archived(const char *raw_path_with_query) {
    const char *q = strchr(raw_path_with_query, '?');
    if (!q) return 0;
    q++;
    while (*q) {
        const char *amp = strchr(q, '&');
        size_t seglen = amp ? (size_t)(amp - q) : strlen(q);
        if (seglen > 15 && strncmp(q, "allow_archived=", 15) == 0) {
            const char *v = q + 15;
            size_t vlen = seglen - 15;
            if ((vlen == 4 && strncmp(v, "true", 4) == 0) || (vlen == 1 && v[0] == '1')) return 1;
            return 0;
        }
        if (!amp) break;
        q = amp + 1;
    }
    return 0;
}

/* ------------------------------------------------------------------------ */
/* Соединения: состояние, буферы, ввод-вывод                                */
/* ------------------------------------------------------------------------ */

typedef enum { CS_HTTP_HEADERS, CS_HTTP_BODY, CS_HTTP_CLOSING, CS_WS } conn_state_t;

typedef struct {
    int fd;
    int in_use;
    conn_state_t state;

    /* Who is on the other end. "Local" = 127.0.0.1 or the machine's own address
       (Moonraker, HelixScreen, scripts on the printer); everything else counts as
       a LAN client and is subject to the per-peer and pool limits below. */
    int is_local;
    struct in_addr peer;
    double accepted_mono;     /* the HTTP deadline counts from here, not from the last byte */

    char *rx; size_t rx_len, rx_cap;
    size_t header_body_offset;
    long content_length;
    HttpRequestMeta meta;

    char *tx; size_t tx_len, tx_cap, tx_sent;
    int close_after_flush;
    int close_now;            /* drop without flushing (buffer cap hit, protocol error) */
    int rd_eof;               /* peer shut down its sending side; stop polling POLLIN */

    int is_ws;
    double last_ping_sent_mono;
    double last_rx_mono;      /* last byte from a websocket peer (pong, ping, anything) */
    int ws_state; /* 0=заголовок кадра, 1=payload управляющего кадра, 2=съедаем данные */
    unsigned char ws_hdrbuf[14];
    int ws_hdrlen;
    int ws_fin, ws_opcode, ws_masked;
    uint64_t ws_paylen;
    unsigned char ws_mask[4];
    unsigned char ws_ctrlbuf[125];
    int ws_ctrl_recv;
    uint64_t ws_discard_remaining;
} Conn;

static Conn g_conns[MAX_CONNS];

/* Слот соединения переиспользуется многократно (см. MAX_CONNS). Small rx/tx
   buffers (up to BUF_KEEP_CAP) stay with the slot for the next client, which
   avoids malloc/free churn on a stream of short HTTP requests (otherwise RSS
   creeps up from fragmentation on small allocators). Anything bigger, e.g. after
   a large spool list or a websocket burst, is freed, so idle memory stays at
   MAX_CONNS × 2 × BUF_KEEP_CAP at most. */
static void conn_clear_keep_buffers(Conn *c) {
    if (c->rx_cap > BUF_KEEP_CAP) { free(c->rx); c->rx = NULL; c->rx_cap = 0; }
    if (c->tx_cap > BUF_KEEP_CAP) { free(c->tx); c->tx = NULL; c->tx_cap = 0; }
    char *saved_rx = c->rx; size_t saved_rx_cap = c->rx_cap;
    char *saved_tx = c->tx; size_t saved_tx_cap = c->tx_cap;
    memset(c, 0, sizeof *c);
    c->fd = -1;
    c->rx = saved_rx; c->rx_cap = saved_rx_cap;
    c->tx = saved_tx; c->tx_cap = saved_tx_cap;
}

static void conn_reset(Conn *c) {
    if (c->fd >= 0) close(c->fd);
    conn_clear_keep_buffers(c);
}

static int conn_rx_reserve(Conn *c, size_t extra) {
    if (c->rx_len + extra + 1 <= c->rx_cap) return 0;
    size_t newcap = c->rx_cap ? c->rx_cap * 2 : 1024;
    while (newcap < c->rx_len + extra + 1) newcap *= 2;
    char *n = realloc(c->rx, newcap);
    if (!n) return -1;
    c->rx = n; c->rx_cap = newcap;
    return 0;
}

/* Memory held by all send buffers except `except`'s. */
static size_t tx_total_cap(const Conn *except) {
    size_t total = 0;
    for (int i = 0; i < MAX_CONNS; i++) if (&g_conns[i] != except) total += g_conns[i].tx_cap;
    return total;
}

/* Queues bytes for sending. Fails (-1) instead of growing past the caps: a
   websocket peer may have at most WS_TX_LIMIT unsent bytes queued (a client that
   never reads cannot grow our memory via ping echoes or broadcasts), and all
   send buffers together stay under TX_TOTAL_LIMIT. Callers drop the connection
   or answer 503 on failure. */
static int conn_tx_append(Conn *c, const void *data, size_t len) {
    if (len == 0) return 0;
    size_t pending = c->tx_len - c->tx_sent;
    size_t limit = c->is_ws ? WS_TX_LIMIT : TX_TOTAL_LIMIT;
    if (len > limit || pending > limit - len) return -1;
    if (c->tx_len + len > c->tx_cap && c->tx_sent > 0) {
        /* reclaim the already-sent head before growing */
        memmove(c->tx, c->tx + c->tx_sent, pending);
        c->tx_len = pending;
        c->tx_sent = 0;
    }
    if (c->tx_len + len > c->tx_cap) {
        size_t need = c->tx_len + len;
        size_t newcap = c->tx_cap ? c->tx_cap * 2 : 1024;
        if (newcap < need) newcap = (need + 1023) & ~(size_t)1023;
        if (newcap > TX_TOTAL_LIMIT) newcap = need;
        if (tx_total_cap(c) + newcap > TX_TOTAL_LIMIT) return -1;
        char *n = realloc(c->tx, newcap);
        if (!n) return -1;
        c->tx = n; c->tx_cap = newcap;
    }
    memcpy(c->tx + c->tx_len, data, len);
    c->tx_len += len;
    return 0;
}

static void conn_try_flush(Conn *c) {
    if (!c->in_use) return;
    if (c->close_now) { conn_reset(c); return; }
    while (c->tx_sent < c->tx_len) {
        ssize_t w = send(c->fd, c->tx + c->tx_sent, c->tx_len - c->tx_sent, 0);
        if (w > 0) { c->tx_sent += (size_t)w; continue; }
        if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
        if (w < 0 && errno == EINTR) continue;
        conn_reset(c);
        return;
    }
    c->tx_len = 0; c->tx_sent = 0;
    if (c->tx_cap > BUF_KEEP_CAP) { free(c->tx); c->tx = NULL; c->tx_cap = 0; }
    /* Ответ отправлен целиком. Мы всегда шлём `Connection: close`, поэтому закрываем сами, не дожидаясь
       FIN от клиента: иначе слот держится до таймаута, и на серии подряд идущих запросов сервер
       начинает отвергать новые соединения (ловилось в CI на Linux). */
    if (c->close_after_flush || c->state == CS_HTTP_CLOSING) { conn_reset(c); return; }
}

/* ------------------------------------------------------------------------ */
/* HTTP: отправка ответов                                                   */
/* ------------------------------------------------------------------------ */

static const char *status_text_for(int code) {
    switch (code) {
        case 200: return "OK";
        case 101: return "Switching Protocols";
        case 400: return "Bad Request";
        case 404: return "Not Found";
        case 405: return "Method Not Allowed";
        case 411: return "Length Required";
        case 403: return "Forbidden";
        case 413: return "Payload Too Large";
        case 415: return "Unsupported Media Type";
        case 500: return "Internal Server Error";
        case 503: return "Service Unavailable";
        default:  return "Error";
    }
}

/* Queues one complete HTTP response (the send buffer is empty at this point).
   If it does not fit under TX_TOTAL_LIMIT the client gets a short 503 instead,
   and if even that does not fit, the connection is dropped. */
static void http_queue_response(Conn *c, const char *head, size_t hn, const void *body, size_t blen) {
    c->close_after_flush = 1;
    if (conn_tx_append(c, head, hn) == 0 && conn_tx_append(c, body, blen) == 0) return;
    c->tx_len = 0; c->tx_sent = 0;
    static const char busy_body[] = "{\"message\":\"server busy, try again\"}";
    char busy[256];
    int n = snprintf(busy, sizeof busy,
        "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: %zu\r\n"
        "Connection: close\r\n\r\n%s", sizeof busy_body - 1, busy_body);
    if (n < 0 || (size_t)n >= sizeof busy || conn_tx_append(c, busy, (size_t)n) != 0) c->close_now = 1;
}

static void http_send_json(Conn *c, int status, cJSON *body_owned, int total_count) {
    char *json_text = body_owned ? cJSON_PrintUnformatted(body_owned) : NULL;
    if (body_owned) cJSON_Delete(body_owned);
    if (body_owned && !json_text) status = 503;   /* out of memory while printing */
    size_t blen = json_text ? strlen(json_text) : 0;
    char head[512];
    int hn;
    if (total_count >= 0) {
        hn = snprintf(head, sizeof head,
            "HTTP/1.1 %d %s\r\nContent-Type: application/json\r\nX-Content-Type-Options: nosniff\r\nContent-Length: %zu\r\nX-Total-Count: %d\r\nConnection: close\r\n\r\n",
            status, status_text_for(status), blen, total_count);
    } else {
        hn = snprintf(head, sizeof head,
            "HTTP/1.1 %d %s\r\nContent-Type: application/json\r\nX-Content-Type-Options: nosniff\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n",
            status, status_text_for(status), blen);
    }
    http_queue_response(c, head, (size_t)hn, json_text, blen);
    free(json_text);
}

static void http_send_error(Conn *c, int status, const char *msg) {
    cJSON *o = cJSON_CreateObject();
    cJSON_AddStringToObject(o, "message", msg);
    http_send_json(c, status, o, -1);
}

/* ------------------------------------------------------------------------ */
/* WebSocket: handshake, кадры, рассылка событий                            */
/* ------------------------------------------------------------------------ */

/* Queues one frame; a peer whose unsent backlog would pass WS_TX_LIMIT is
   marked for dropping (close_now) rather than buffered without bound. */
static int ws_send_frame(Conn *c, int opcode, const unsigned char *payload, size_t len) {
    unsigned char hdr[10];
    size_t hn = 0;
    hdr[hn++] = (unsigned char)(0x80 | (opcode & 0x0F));
    if (len < 126) {
        hdr[hn++] = (unsigned char)len;
    } else if (len <= 0xFFFF) {
        hdr[hn++] = 126;
        hdr[hn++] = (unsigned char)(len >> 8);
        hdr[hn++] = (unsigned char)(len);
    } else {
        hdr[hn++] = 127;
        /* uint64_t: with a 32-bit size_t (MIPS32, ARMv7) shifting by 32..56 bits is undefined */
        for (int i = 7; i >= 0; i--) hdr[hn++] = (unsigned char)((uint64_t)len >> (8 * i));
    }
    if (len > WS_TX_LIMIT || conn_tx_append(c, hdr, hn) != 0 || conn_tx_append(c, payload, len) != 0) {
        log_v("websocket peer too slow (%zu bytes unsent), dropping it", c->tx_len - c->tx_sent);
        c->close_now = 1;
        return -1;
    }
    return 0;
}

static void ws_send_handshake(Conn *c, const char *key) {
    char accept[64];
    ws_compute_accept(key, accept);
    char head[256];
    int n = snprintf(head, sizeof head,
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n",
        accept);
    c->is_ws = 1;
    c->state = CS_WS;
    c->close_after_flush = 0;
    if (n < 0 || (size_t)n >= sizeof head || conn_tx_append(c, head, (size_t)n) != 0) c->close_now = 1;
    c->last_ping_sent_mono = mono_now();
    c->last_rx_mono = mono_now();
    c->ws_hdrlen = 0;
    c->ws_state = 0;
}

static void ws_handle_ctrl_frame(Conn *c) {
    for (uint64_t i = 0; i < c->ws_paylen; i++) c->ws_ctrlbuf[i] ^= c->ws_mask[i % 4];
    if (c->ws_opcode == 0x9) {
        ws_send_frame(c, 0xA, c->ws_ctrlbuf, (size_t)c->ws_paylen);
    } else if (c->ws_opcode == 0x8) {
        ws_send_frame(c, 0x8, c->ws_ctrlbuf, (size_t)c->ws_paylen);
        c->close_after_flush = 1;
    } /* 0xA (pong) от клиента — игнорируем */
}

static void ws_feed(Conn *c, const unsigned char *data, size_t n) {
    size_t i = 0;
    while (i < n) {
        if (c->close_now || c->close_after_flush) return;   /* closing: ignore the rest */
        if (c->ws_state == 0) {
            if (c->ws_hdrlen < 2) {
                c->ws_hdrbuf[c->ws_hdrlen++] = data[i++];
                continue;
            }
            int base_len = c->ws_hdrbuf[1] & 0x7F;
            int masked = (c->ws_hdrbuf[1] >> 7) & 1;
            int need = 2;
            if (base_len == 126) need += 2;
            else if (base_len == 127) need += 8;
            if (masked) need += 4;
            if (c->ws_hdrlen < need) {
                c->ws_hdrbuf[c->ws_hdrlen++] = data[i++];
                if (c->ws_hdrlen < need) continue;
            }
            /* заголовок кадра собран целиком */
            c->ws_fin = (c->ws_hdrbuf[0] >> 7) & 1;
            c->ws_opcode = c->ws_hdrbuf[0] & 0x0F;
            c->ws_masked = masked;
            int off = 2;
            uint64_t paylen;
            if (base_len == 126) {
                paylen = ((uint64_t)c->ws_hdrbuf[2] << 8) | c->ws_hdrbuf[3];
                off += 2;
            } else if (base_len == 127) {
                paylen = 0;
                for (int k = 0; k < 8; k++) paylen = (paylen << 8) | c->ws_hdrbuf[2 + k];
                off += 8;
            } else {
                paylen = (uint64_t)base_len;
            }
            if (masked) { memcpy(c->ws_mask, c->ws_hdrbuf + off, 4); off += 4; }
            else memset(c->ws_mask, 0, 4);
            (void)off;
            c->ws_paylen = paylen;
            c->ws_hdrlen = 0;

            if (c->ws_opcode == 0x8 || c->ws_opcode == 0x9 || c->ws_opcode == 0xA) {
                if (paylen > 125 || !masked) {
                    unsigned char code[2] = { 0x03, 0xEA }; /* 1002 protocol error */
                    ws_send_frame(c, 0x8, code, 2);
                    c->close_after_flush = 1;
                    return;
                }
                c->ws_ctrl_recv = 0;
                if (paylen == 0) { ws_handle_ctrl_frame(c); c->ws_state = 0; }
                else c->ws_state = 1;
            } else {
                c->ws_discard_remaining = paylen;
                c->ws_state = (paylen == 0) ? 0 : 2;
            }
            continue;
        } else if (c->ws_state == 1) {
            size_t remain = (size_t)c->ws_paylen - (size_t)c->ws_ctrl_recv;
            size_t take = n - i; if (take > remain) take = remain;
            memcpy(c->ws_ctrlbuf + c->ws_ctrl_recv, data + i, take);
            c->ws_ctrl_recv += (int)take;
            i += take;
            if ((uint64_t)c->ws_ctrl_recv == c->ws_paylen) {
                ws_handle_ctrl_frame(c);
                c->ws_state = 0;
            }
        } else {
            uint64_t take = n - i;
            if (take > c->ws_discard_remaining) take = c->ws_discard_remaining;
            i += take;
            c->ws_discard_remaining -= take;
            if (c->ws_discard_remaining == 0) c->ws_state = 0;
        }
    }
}

static void ws_broadcast_event(const char *type, const char *resource, cJSON *payload_owned) {
    cJSON *evt = cJSON_CreateObject();
    cJSON_AddStringToObject(evt, "type", type);
    cJSON_AddStringToObject(evt, "resource", resource);
    char ts[32]; now_iso(ts, sizeof ts);
    cJSON_AddStringToObject(evt, "date", ts);
    cJSON_AddItemToObject(evt, "payload", payload_owned);
    char *text = cJSON_PrintUnformatted(evt);
    cJSON_Delete(evt);
    if (!text) return;
    size_t len = strlen(text);
    for (int i = 0; i < MAX_CONNS; i++) {
        if (g_conns[i].in_use && g_conns[i].is_ws && !g_conns[i].close_after_flush) {
            ws_send_frame(&g_conns[i], 0x1, (const unsigned char *)text, len);
            conn_try_flush(&g_conns[i]);
        }
    }
    free(text);
}

/* ------------------------------------------------------------------------ */
/* Отложенный сброс данных для PUT .../use                                  */
/* ------------------------------------------------------------------------ */

static void schedule_use_flush(void) {
    double now = mono_now();
    if (now - g_last_use_flush_mono >= USE_FLUSH_INTERVAL_S) {
        save_data();
    } else {
        g_use_pending = 1;
        double candidate = g_last_use_flush_mono + USE_FLUSH_INTERVAL_S;
        if (!g_use_flush_deadline || candidate < g_use_flush_deadline) g_use_flush_deadline = candidate;
    }
}

/* ------------------------------------------------------------------------ */
/* Маршрутизация HTTP-запросов                                              */
/* ------------------------------------------------------------------------ */

static void route_and_dispatch(Conn *c, HttpRequestMeta *m, const char *body, size_t body_len) {
    char path[1024];
    split_path(m->path, path, sizeof path);
    log_v("%s %s (%zu байт тела)", m->method, m->path, body_len);

    if (strcmp(path, "/api/v1/info") == 0) {
        if (strcmp(m->method, "GET") != 0) { http_send_error(c, 405, "method not allowed"); return; }
        cJSON *o = cJSON_CreateObject();
        cJSON_AddStringToObject(o, "version", SERVER_VERSION);
        cJSON_AddStringToObject(o, "microspool_version", MICROSPOOL_VERSION);
        cJSON_AddBoolToObject(o, "debug_mode", 0);
        cJSON_AddBoolToObject(o, "automatic_backups", 0);
        /* realpath() пишет до PATH_MAX (на Linux 4096, на macOS 1024) — буфер меньше PATH_MAX
           означает затирание стека, поэтому просим glibc/musl выделить память сами (POSIX.1-2008). */
        char *resolved = realpath(g_data_path, NULL);
        char abspath[PATH_MAX];
        snprintf(abspath, sizeof abspath, "%s", resolved ? resolved : g_data_path);
        free(resolved);
        cJSON_AddStringToObject(o, "data_dir", abspath);
        cJSON_AddStringToObject(o, "backups_dir", "");
        cJSON_AddStringToObject(o, "db_type", "microspool");
        cJSON_AddNullToObject(o, "git_commit");
        cJSON_AddNullToObject(o, "build_date");
        http_send_json(c, 200, o, -1);
        return;
    }
    if (strcmp(path, "/api/v1/health") == 0) {
        if (strcmp(m->method, "GET") != 0) { http_send_error(c, 405, "method not allowed"); return; }
        cJSON *o = cJSON_CreateObject();
        cJSON_AddStringToObject(o, "status", "healthy");
        http_send_json(c, 200, o, -1);
        return;
    }
    if (strcmp(path, "/api/v1/external/vendor") == 0 || strcmp(path, "/api/v1/external/filament") == 0) {
        if (strcmp(m->method, "GET") != 0) { http_send_error(c, 405, "method not allowed"); return; }
        http_send_json(c, 200, cJSON_CreateArray(), 0);
        return;
    }
    if (strcmp(path, "/api/v1/setting/currency") == 0) {
        if (strcmp(m->method, "GET") != 0) { http_send_error(c, 405, "method not allowed"); return; }
        cJSON *o = cJSON_CreateObject();
        cJSON_AddStringToObject(o, "value", "\"EUR\"");
        cJSON_AddBoolToObject(o, "is_set", 0);
        cJSON_AddStringToObject(o, "type", "string");
        http_send_json(c, 200, o, -1);
        return;
    }
    if (strncmp(path, "/api/v1/setting/", 16) == 0) {
        http_send_error(c, 404, "not found");
        return;
    }
    if (strcmp(path, "/api/v1/material") == 0) {
        if (strcmp(m->method, "GET") != 0) { http_send_error(c, 405, "method not allowed"); return; }
        const char *items[MAX_FILAMENTS];
        int n = 0;
        for (int i = 0; i < MAX_FILAMENTS; i++) {
            if (g_filaments[i].id && g_filaments[i].has_material && g_filaments[i].material[0]) {
                items[n++] = g_filaments[i].material;
            }
        }
        cJSON *arr = unique_sorted_strings(items, n);
        http_send_json(c, 200, arr, cJSON_GetArraySize(arr));
        return;
    }
    if (strcmp(path, "/api/v1/location") == 0) {
        if (strcmp(m->method, "GET") != 0) { http_send_error(c, 405, "method not allowed"); return; }
        const char *items[MAX_SPOOLS];
        int n = 0;
        for (int i = 0; i < MAX_SPOOLS; i++) {
            if (g_spools[i].id && g_spools[i].has_location && g_spools[i].location[0]) {
                items[n++] = g_spools[i].location;
            }
        }
        cJSON *arr = unique_sorted_strings(items, n);
        http_send_json(c, 200, arr, cJSON_GetArraySize(arr));
        return;
    }
    if (strcmp(path, "/api/v1/lot-number") == 0) {
        if (strcmp(m->method, "GET") != 0) { http_send_error(c, 405, "method not allowed"); return; }
        const char *items[MAX_SPOOLS];
        int n = 0;
        for (int i = 0; i < MAX_SPOOLS; i++) {
            if (g_spools[i].id && g_spools[i].has_lot_nr && g_spools[i].lot_nr[0]) {
                items[n++] = g_spools[i].lot_nr;
            }
        }
        cJSON *arr = unique_sorted_strings(items, n);
        http_send_json(c, 200, arr, cJSON_GetArraySize(arr));
        return;
    }
    if (strcmp(path, "/") == 0 || strcmp(path, "/index.html") == 0) {
        if (!g_ui_enabled) { http_send_error(c, 404, "not found"); return; }
        if (strcmp(m->method, "GET") != 0) { http_send_error(c, 405, "method not allowed"); return; }
        if (!m->accepts_gzip) {
            static const char msg[] =
                "406 Not Acceptable: this UI is served pre-compressed; "
                "request it with \"Accept-Encoding: gzip\".";
            char head[256];
            int hn = snprintf(head, sizeof head,
                "HTTP/1.1 406 Not Acceptable\r\nContent-Type: text/plain; charset=utf-8\r\nX-Content-Type-Options: nosniff\r\n"
                "Content-Length: %zu\r\nConnection: close\r\n\r\n", sizeof msg - 1);
            http_queue_response(c, head, (size_t)hn, msg, sizeof msg - 1);
            return;
        }
        /* CSP: only the page's own inline script (by hash, from embed_ui.py) may run;
           inline style attributes are used by the UI, hence 'unsafe-inline' for styles.
           connect-src must reach Moonraker for the active-spool badge: by default
           http://<host>:7125, but the URL is configurable in the UI, so any
           http(s) endpoint is allowed for fetch(). */
        char head[1024];
        int hn = snprintf(head, sizeof head,
            "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
            "Content-Encoding: gzip\r\nCache-Control: no-cache\r\n"
            "X-Content-Type-Options: nosniff\r\nX-Frame-Options: DENY\r\nReferrer-Policy: no-referrer\r\n"
            "Content-Security-Policy: default-src 'none'; script-src " UI_CSP_SCRIPT_SRC "; "
            "style-src 'unsafe-inline'; img-src 'self' data:; connect-src 'self' http: https:; "
            "base-uri 'none'; form-action 'none'; frame-ancestors 'none'\r\n"
            "Content-Length: %u\r\nConnection: close\r\n\r\n",
            UI_INDEX_GZ_LEN);
        http_queue_response(c, head, (size_t)hn, UI_INDEX_GZ, UI_INDEX_GZ_LEN);
        return;
    }

    int need_input = str_ieq(m->method, "POST") || str_ieq(m->method, "PATCH") || str_ieq(m->method, "PUT");
    cJSON *input = NULL;
    if (need_input) {
        if (body_len == 0) {
            input = cJSON_CreateObject();
        } else {
            if (!utf8_valid((const unsigned char *)body, body_len)) {
                http_send_error(c, 400, "request body is not valid UTF-8");
                return;
            }
            input = cJSON_ParseWithLength(body, body_len);
            if (!input || !cJSON_IsObject(input)) {
                if (input) cJSON_Delete(input);
                http_send_error(c, 400, "invalid JSON body");
                return;
            }
        }
    }

    if (strcmp(path, "/api/v1/vendor") == 0) {
        if (strcmp(m->method, "GET") == 0) {
            cJSON *arr; int cnt; vendor_list(&arr, &cnt);
            http_send_json(c, 200, arr, cnt);
        } else if (strcmp(m->method, "POST") == 0) {
            cJSON *out = NULL; char msg[300];
            int rc = vendor_create(input, &out, msg, sizeof msg);
            if (rc) http_send_error(c, rc, msg);
            else { http_send_json(c, 200, out, -1); save_data(); }
        } else {
            http_send_error(c, 405, "method not allowed");
        }
        goto done;
    }
    if (strncmp(path, "/api/v1/vendor/", 15) == 0) {
        const char *idstr = path + 15;
        char *endp;
        long idl = (*idstr) ? strtol(idstr, &endp, 10) : -1;
        int bad = (*idstr == 0) || (*endp != 0) || idl <= 0;
        if (bad) { http_send_error(c, 404, "vendor not found"); goto done; }
        int id = (int)idl;
        cJSON *out = NULL; char msg[300]; int rc;
        if (strcmp(m->method, "GET") == 0) {
            rc = vendor_get(id, &out, msg, sizeof msg);
            if (rc) http_send_error(c, rc, msg); else http_send_json(c, 200, out, -1);
        } else if (strcmp(m->method, "PATCH") == 0) {
            rc = vendor_patch(id, input, &out, msg, sizeof msg);
            if (rc) http_send_error(c, rc, msg); else { http_send_json(c, 200, out, -1); save_data(); }
        } else if (strcmp(m->method, "DELETE") == 0) {
            rc = vendor_delete(id, &out, msg, sizeof msg);
            if (rc) http_send_error(c, rc, msg); else { http_send_json(c, 200, out, -1); save_data(); }
        } else {
            http_send_error(c, 405, "method not allowed");
        }
        goto done;
    }

    if (strcmp(path, "/api/v1/filament") == 0) {
        if (strcmp(m->method, "GET") == 0) {
            cJSON *arr; int cnt; filament_list(&arr, &cnt);
            http_send_json(c, 200, arr, cnt);
        } else if (strcmp(m->method, "POST") == 0) {
            cJSON *out = NULL; char msg[300];
            int rc = filament_create(input, &out, msg, sizeof msg);
            if (rc) http_send_error(c, rc, msg);
            else { http_send_json(c, 200, out, -1); save_data(); }
        } else {
            http_send_error(c, 405, "method not allowed");
        }
        goto done;
    }
    if (strncmp(path, "/api/v1/filament/", 17) == 0) {
        const char *idstr = path + 17;
        char *endp;
        long idl = (*idstr) ? strtol(idstr, &endp, 10) : -1;
        int bad = (*idstr == 0) || (*endp != 0) || idl <= 0;
        if (bad) { http_send_error(c, 404, "filament not found"); goto done; }
        int id = (int)idl;
        cJSON *out = NULL; char msg[300]; int rc;
        if (strcmp(m->method, "GET") == 0) {
            rc = filament_get(id, &out, msg, sizeof msg);
            if (rc) http_send_error(c, rc, msg); else http_send_json(c, 200, out, -1);
        } else if (strcmp(m->method, "PATCH") == 0) {
            rc = filament_patch(id, input, &out, msg, sizeof msg);
            if (rc) http_send_error(c, rc, msg); else { http_send_json(c, 200, out, -1); save_data(); }
        } else if (strcmp(m->method, "DELETE") == 0) {
            rc = filament_delete(id, &out, msg, sizeof msg);
            if (rc) http_send_error(c, rc, msg); else { http_send_json(c, 200, out, -1); save_data(); }
        } else {
            http_send_error(c, 405, "method not allowed");
        }
        goto done;
    }

    if (strcmp(path, "/api/v1/spool") == 0) {
        if (strcmp(m->method, "GET") == 0) {
            int allow_archived = query_allow_archived(m->path);
            cJSON *arr; int cnt; spool_list(allow_archived, &arr, &cnt);
            http_send_json(c, 200, arr, cnt);
        } else if (strcmp(m->method, "POST") == 0) {
            cJSON *out = NULL; char msg[300];
            int rc = spool_create(input, &out, msg, sizeof msg);
            if (rc) http_send_error(c, rc, msg);
            else {
                cJSON *bcast = cJSON_Duplicate(out, 1);
                http_send_json(c, 200, out, -1);
                save_data();
                ws_broadcast_event("added", "spool", bcast);
            }
        } else {
            http_send_error(c, 405, "method not allowed");
        }
        goto done;
    }
    if (strncmp(path, "/api/v1/spool/", 14) == 0) {
        const char *rest = path + 14;
        char idpart[64]; const char *slash = strchr(rest, '/');
        int is_use = 0;
        if (slash) {
            size_t idlen = (size_t)(slash - rest);
            if (idlen == 0 || idlen >= sizeof idpart) { http_send_error(c, 404, "spool not found"); goto done; }
            memcpy(idpart, rest, idlen); idpart[idlen] = 0;
            if (strcmp(slash, "/use") == 0) is_use = 1;
            else { http_send_error(c, 404, "not found"); goto done; }
        } else {
            strncpy(idpart, rest, sizeof idpart - 1);
            idpart[sizeof idpart - 1] = 0;
        }
        char *endp;
        long idl = idpart[0] ? strtol(idpart, &endp, 10) : -1;
        int bad = (idpart[0] == 0) || (*endp != 0) || idl <= 0;
        if (bad) { http_send_error(c, 404, "spool not found"); goto done; }
        int id = (int)idl;
        cJSON *out = NULL; char msg[300]; int rc;
        if (is_use) {
            if (strcmp(m->method, "PUT") != 0) { http_send_error(c, 405, "method not allowed"); goto done; }
            rc = spool_use(id, input, &out, msg, sizeof msg);
            if (rc) http_send_error(c, rc, msg);
            else {
                cJSON *bcast = cJSON_Duplicate(out, 1);
                http_send_json(c, 200, out, -1);
                schedule_use_flush();
                ws_broadcast_event("updated", "spool", bcast);
            }
            goto done;
        }
        if (strcmp(m->method, "GET") == 0) {
            rc = spool_get(id, &out, msg, sizeof msg);
            if (rc) http_send_error(c, rc, msg); else http_send_json(c, 200, out, -1);
        } else if (strcmp(m->method, "PATCH") == 0) {
            rc = spool_patch(id, input, &out, msg, sizeof msg);
            if (rc) http_send_error(c, rc, msg);
            else {
                cJSON *bcast = cJSON_Duplicate(out, 1);
                http_send_json(c, 200, out, -1);
                save_data();
                ws_broadcast_event("updated", "spool", bcast);
            }
        } else if (strcmp(m->method, "DELETE") == 0) {
            rc = spool_delete(id, &out, msg, sizeof msg);
            if (rc) http_send_error(c, rc, msg);
            else {
                cJSON *bcast = cJSON_Duplicate(out, 1);
                http_send_json(c, 200, out, -1);
                save_data();
                ws_broadcast_event("deleted", "spool", bcast);
            }
        } else {
            http_send_error(c, 405, "method not allowed");
        }
        goto done;
    }

    http_send_error(c, 404, "not found");

done:
    if (input) cJSON_Delete(input);
}

/* ------------------------------------------------------------------------ */
/* Request checks against hostile web pages                                 */
/*                                                                          */
/* The UI and API listen on the LAN without a login, so any web page open   */
/* in a LAN browser can send requests here. Three checks keep it out:       */
/*  - Host allowlist (DNS rebinding): the Host header must be an IP         */
/*    literal, localhost, this machine's hostname (or hostname.local), or a  */
/*    name given with -H. Moonraker uses http://127.0.0.1:7912, so its Host  */
/*    is "127.0.0.1:7912". Requests without a Host header (HTTP/1.0 tools)   */
/*    pass.                                                                 */
/*  - Origin: websocket upgrades and every method other than GET/HEAD must  */
/*    come without an Origin header (non-browser clients such as Moonraker) */
/*    or from the same origin as the Host they were sent to, or from an     */
/*    origin given with -o.                                                 */
/*  - Content-Type: a POST/PATCH/PUT with a body must say application/json; */
/*    a cross-site form or text/plain POST is refused before parsing.       */
/* ------------------------------------------------------------------------ */

static void str_lower(char *s) { for (; *s; s++) *s = (char)tolower((unsigned char)*s); }

/* Splits "name[:port]" or "[v6][:port]" into a lower-case name without a
   trailing dot. Returns -1 if it does not look like a host. */
static int host_name_part(const char *hostport, char *out, size_t outsz) {
    const char *end;
    if (hostport[0] == '[') {
        end = strchr(hostport, ']');
        if (!end) return -1;
        end++;
    } else {
        end = strchr(hostport, ':');
        if (!end) end = hostport + strlen(hostport);
    }
    if (*end == ':') {
        if (!end[1]) return -1;
        for (const char *p = end + 1; *p; p++) if (!isdigit((unsigned char)*p)) return -1;
    } else if (*end) {
        return -1;
    }
    size_t n = (size_t)(end - hostport);
    if (n == 0 || n >= outsz) return -1;
    memcpy(out, hostport, n);
    out[n] = 0;
    if (n > 1 && out[n - 1] == '.') out[n - 1] = 0;
    str_lower(out);
    return 0;
}

static int host_allowed(const char *hostport) {
    if (g_allow_any_host) return 1;
    char name[256];
    if (host_name_part(hostport, name, sizeof name) != 0) return 0;
    if (name[0] == '[') {
        char v6[64];
        size_t n = strlen(name);
        if (n < 3 || n - 2 >= sizeof v6) return 0;
        memcpy(v6, name + 1, n - 2); v6[n - 2] = 0;
        struct in6_addr a6;
        return inet_pton(AF_INET6, v6, &a6) == 1;
    }
    struct in_addr a4;
    if (inet_pton(AF_INET, name, &a4) == 1) return 1;
    if (strcmp(name, "localhost") == 0) return 1;
    if (g_hostname[0]) {
        size_t hl = strlen(g_hostname);
        if (strcmp(name, g_hostname) == 0) return 1;
        if (strncmp(name, g_hostname, hl) == 0 && strcmp(name + hl, ".local") == 0) return 1;
    }
    for (int i = 0; i < g_allow_hosts_n; i++) if (str_ieq(name, g_allow_hosts[i])) return 1;
    return 0;
}

/* "scheme://host[:port]" -> lower-case "host:port" with the default port made
   explicit, so "http://a" and Host "a:80" compare equal. */
static int origin_authority(const char *scheme, const char *hostport, char *out, size_t outsz) {
    char name[256];
    if (host_name_part(hostport, name, sizeof name) != 0) return -1;
    const char *port = hostport[0] == '[' ? strchr(hostport, ']') + 1 : strchr(hostport, ':');
    const char *p = (port && *port == ':') ? port + 1 : (strcmp(scheme, "https") == 0 ? "443" : "80");
    int n = snprintf(out, outsz, "%s:%s", name, p);
    return (n < 0 || (size_t)n >= outsz) ? -1 : 0;
}

static int origin_allowed(const HttpRequestMeta *m) {
    if (!m->has_origin) return 1;
    char want[300];
    snprintf(want, sizeof want, "%s", m->origin);
    size_t wl = strlen(want);
    if (wl && want[wl - 1] == '/') want[wl - 1] = 0;
    for (int i = 0; i < g_allow_origins_n; i++) if (str_ieq(want, g_allow_origins[i])) return 1;
    const char *sep = strstr(want, "://");
    if (!sep || !m->has_host) return 0;   /* includes Origin: null */
    char scheme[8];
    size_t sl = (size_t)(sep - want);
    if (sl == 0 || sl >= sizeof scheme) return 0;
    memcpy(scheme, want, sl); scheme[sl] = 0;
    str_lower(scheme);
    if (strcmp(scheme, "http") != 0 && strcmp(scheme, "https") != 0) return 0;
    char a[300], b[300];
    if (origin_authority(scheme, sep + 3, a, sizeof a) != 0) return 0;
    if (origin_authority(scheme, m->host, b, sizeof b) != 0) return 0;
    return strcmp(a, b) == 0;
}

static int content_type_is_json(const char *ct) {
    char media[128];
    size_t n = strcspn(ct, ";");
    while (n > 0 && (ct[n - 1] == ' ' || ct[n - 1] == '\t')) n--;
    if (n >= sizeof media) return 0;
    memcpy(media, ct, n); media[n] = 0;
    return str_ieq(media, "application/json");
}

/* 0 = go ahead, otherwise an HTTP status has been queued on c. */
static int request_rejected(Conn *c, const HttpRequestMeta *m) {
    if (m->has_host && !host_allowed(m->host)) {
        log_v("refused Host: %s", m->host);
        http_send_error(c, 403, "host not allowed (start microspool with -H <name> to allow it)");
        return 403;
    }
    int safe_method = str_ieq(m->method, "GET") || str_ieq(m->method, "HEAD");
    if ((m->is_ws_upgrade || !safe_method) && !origin_allowed(m)) {
        log_v("refused Origin: %s", m->origin);
        http_send_error(c, 403, "origin not allowed (start microspool with -o <origin> to allow it)");
        return 403;
    }
    int has_body = m->has_content_length && m->content_length > 0;
    int body_method = str_ieq(m->method, "POST") || str_ieq(m->method, "PATCH") || str_ieq(m->method, "PUT");
    if (body_method && has_body && !content_type_is_json(m->content_type)) {
        http_send_error(c, 415, "Content-Type must be application/json");
        return 415;
    }
    return 0;
}

/* ------------------------------------------------------------------------ */
/* Обработка входящих байт на HTTP-соединении                               */
/* ------------------------------------------------------------------------ */

static void process_http_buffer(Conn *c) {
    if (c->state == CS_HTTP_HEADERS) {
        const char *hdr_end = find_header_end(c->rx, c->rx_len);
        if (!hdr_end) {
            if (c->rx_len > HEADER_LIMIT) {
                http_send_error(c, 400, "request headers too large");
                c->state = CS_HTTP_CLOSING;
                conn_try_flush(c);
            }
            return;
        }
        size_t hdr_len = (size_t)(hdr_end - c->rx);
        HttpRequestMeta meta;
        if (parse_http_request_line_and_headers(c->rx, hdr_len, &meta) != 0) {
            http_send_error(c, 400, "malformed request");
            c->state = CS_HTTP_CLOSING;
            conn_try_flush(c);
            return;
        }

        if (request_rejected(c, &meta)) {
            c->state = CS_HTTP_CLOSING;
            conn_try_flush(c);
            return;
        }

        char path_only[1024];
        split_path(meta.path, path_only, sizeof path_only);
        if (meta.is_ws_upgrade && str_ieq(meta.method, "GET") && strncmp(path_only, "/api/v1/", 8) == 0) {
            int remote_ws = 0;
            for (int i = 0; i < MAX_CONNS; i++)
                if (g_conns[i].in_use && g_conns[i].is_ws && !g_conns[i].is_local) remote_ws++;
            if (meta.ws_key[0] == 0) {
                http_send_error(c, 400, "missing Sec-WebSocket-Key");
                c->state = CS_HTTP_CLOSING;
                conn_try_flush(c);
                return;
            }
            if (!c->is_local && remote_ws >= MAX_REMOTE_WS) {
                http_send_error(c, 503, "too many websocket clients");
                c->state = CS_HTTP_CLOSING;
                conn_try_flush(c);
                return;
            }
            ws_send_handshake(c, meta.ws_key);
            conn_try_flush(c);
            return;
        }

        if (meta.chunked && !meta.has_content_length) {
            http_send_error(c, 411, "Content-Length required (chunked not supported)");
            c->state = CS_HTTP_CLOSING;
            conn_try_flush(c);
            return;
        }
        long clen = meta.has_content_length ? meta.content_length : 0;
        if (clen < 0) clen = 0;
        if (clen > BODY_LIMIT) {
            http_send_error(c, 413, "request body too large");
            c->state = CS_HTTP_CLOSING;
            conn_try_flush(c);
            return;
        }
        c->header_body_offset = hdr_len + 4;
        c->content_length = clen;
        c->meta = meta;
        c->state = CS_HTTP_BODY;
    }
    if (c->state == CS_HTTP_BODY) {
        size_t need = c->header_body_offset + (size_t)c->content_length;
        if (c->rx_len < need) return;
        const char *body = c->rx + c->header_body_offset;
        route_and_dispatch(c, &c->meta, body, (size_t)c->content_length);
        c->state = CS_HTTP_CLOSING;
        conn_try_flush(c);
    }
}

static void conn_on_readable(Conn *c) {
    unsigned char buf[4096];
    for (;;) {
        ssize_t r = recv(c->fd, buf, sizeof buf, 0);
        if (r > 0) {
            if (c->state == CS_WS) {
                c->last_rx_mono = mono_now();
                ws_feed(c, buf, (size_t)r);
                conn_try_flush(c);
                if (!c->in_use) return;
            } else if (c->state == CS_HTTP_CLOSING) {
                /* The request is complete and answered: read and drop whatever else
                   arrives (bounded by the request deadline) instead of growing rx. */
            } else {
                if (conn_rx_reserve(c, (size_t)r) != 0) { conn_reset(c); return; }
                memcpy(c->rx + c->rx_len, buf, (size_t)r);
                c->rx_len += (size_t)r;
                c->rx[c->rx_len] = 0;
                process_http_buffer(c);
                if (!c->in_use) return;
                if (c->state == CS_WS) {
                    conn_try_flush(c);
                    if (!c->in_use) return;
                }
            }
            if ((size_t)r < sizeof buf) break;
        } else if (r == 0) {
            if (c->state == CS_HTTP_CLOSING && c->tx_len > c->tx_sent) {
                c->rd_eof = 1;   /* half-closed after the request: still deliver the answer */
                return;
            }
            conn_reset(c);
            return;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            if (errno == EINTR) continue;
            conn_reset(c);
            return;
        }
    }
}

static void conn_on_writable(Conn *c) {
    conn_try_flush(c);
}

/* Next moment this connection needs attention (deadline, ping, timeout). */
static double conn_next_deadline(const Conn *c) {
    if (!c->in_use) return 1e300;
    if (!c->is_ws) return c->accepted_mono + HTTP_REQUEST_DEADLINE_S;
    if (g_ws_ping_interval <= 0) return 1e300;
    double ping = c->last_ping_sent_mono + g_ws_ping_interval;
    double dead = c->last_rx_mono + (double)WS_TIMEOUT_PINGS * g_ws_ping_interval;
    return ping < dead ? ping : dead;
}

static void conn_run_timers(Conn *c, double now) {
    if (!c->in_use) return;
    if (!c->is_ws) {
        /* One deadline from accept for headers, body and sending the answer;
           trickling a byte at a time does not extend it (slowloris). */
        if (now - c->accepted_mono >= HTTP_REQUEST_DEADLINE_S) {
            log_v("request deadline passed, closing connection");
            conn_reset(c);
        }
        return;
    }
    if (g_ws_ping_interval <= 0) return;   /* -k 0: no pings, so no idle timeout either */
    if (now - c->last_rx_mono >= (double)WS_TIMEOUT_PINGS * g_ws_ping_interval) {
        /* Tornado (Moonraker) answers every ping with a pong, so a live client
           always sends something within one interval. */
        log_v("websocket peer silent for %.0f s, closing", now - c->last_rx_mono);
        conn_reset(c);
        return;
    }
    if (now - c->last_ping_sent_mono >= g_ws_ping_interval) {
        ws_send_frame(c, 0x9, NULL, 0);
        c->last_ping_sent_mono = now;
        conn_try_flush(c);
    }
}

/* Oldest connection matching the filter that is a candidate for eviction. */
static Conn *conn_pick_victim(int remote_only, int incomplete_only, double min_age, double now) {
    Conn *victim = NULL;
    for (int i = 0; i < MAX_CONNS; i++) {
        Conn *c = &g_conns[i];
        if (!c->in_use || c->is_ws) continue;
        if (remote_only && c->is_local) continue;
        if (incomplete_only && c->state != CS_HTTP_HEADERS && c->state != CS_HTTP_BODY) continue;
        if (now - c->accepted_mono < min_age) continue;
        if (!victim || c->accepted_mono < victim->accepted_mono) victim = c;
    }
    return victim;
}

/* Accepts all pending connections and decides who gets a slot:
   - local peers (127.0.0.1 or the machine's own address) may use every slot and,
     when all are taken, push out the oldest LAN HTTP connection;
   - LAN peers share MAX_CONNS - LOCAL_RESERVED_CONNS slots, at most
     MAX_CONNS_PER_PEER per address; when their pool is full, a new LAN client
     evicts the oldest LAN connection that has not finished sending its request
     within EVICT_MIN_AGE_S (slow or idle sockets), otherwise it is closed. */
static void accept_clients(int lfd) {
    for (;;) {
        struct sockaddr_in cliaddr, locaddr;
        socklen_t clilen = sizeof cliaddr, loclen = sizeof locaddr;
        int cfd = accept(lfd, (struct sockaddr *)&cliaddr, &clilen);
        if (cfd < 0) {
            if (errno == EINTR || errno == ECONNABORTED) continue;
            break;
        }
        int is_local = cliaddr.sin_addr.s_addr == htonl(INADDR_LOOPBACK);
        if (!is_local && getsockname(cfd, (struct sockaddr *)&locaddr, &loclen) == 0 &&
            locaddr.sin_addr.s_addr == cliaddr.sin_addr.s_addr) is_local = 1;

        double now = mono_now();
        int remote = 0, same_peer = 0, free_slot = -1;
        for (int i = 0; i < MAX_CONNS; i++) {
            if (!g_conns[i].in_use) { if (free_slot < 0) free_slot = i; continue; }
            if (g_conns[i].is_local) continue;
            remote++;
            if (g_conns[i].peer.s_addr == cliaddr.sin_addr.s_addr) same_peer++;
        }
        Conn *victim = NULL;
        if (!is_local) {
            if (same_peer >= MAX_CONNS_PER_PEER) {
                log_v("%s: too many connections from this address", inet_ntoa(cliaddr.sin_addr));
                close(cfd);
                continue;
            }
            if (remote >= MAX_CONNS - LOCAL_RESERVED_CONNS || free_slot < 0) {
                victim = conn_pick_victim(1, 1, EVICT_MIN_AGE_S, now);
                if (!victim) { log_v("LAN connection pool full, refusing"); close(cfd); continue; }
            }
        } else if (free_slot < 0) {
            victim = conn_pick_victim(1, 0, 0, now);
            if (!victim) { log_v("all connection slots taken by local clients, refusing"); close(cfd); continue; }
        }
        if (victim) {
            log_v("evicting an old LAN connection to make room");
            conn_reset(victim);
            free_slot = (int)(victim - g_conns);
        }

        fcntl(cfd, F_SETFL, O_NONBLOCK);
        fcntl(cfd, F_SETFD, FD_CLOEXEC);
        int one = 1;
        setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        Conn *c = &g_conns[free_slot];
        conn_clear_keep_buffers(c);   /* keeps the slot's small buffers (see above) */
        c->fd = cfd;
        c->in_use = 1;
        c->is_local = is_local;
        c->peer = cliaddr.sin_addr;
        c->state = CS_HTTP_HEADERS;
        c->accepted_mono = now;
        c->content_length = -1;
    }
}

/* ------------------------------------------------------------------------ */
/* main: аргументы, сокет, сигналы, event loop                              */
/* ------------------------------------------------------------------------ */

static void on_term_signal(int sig) { (void)sig; g_should_exit = 1; }

static void print_usage(const char *argv0) {
    fprintf(stderr,
        "usage: %s [-l addr] [-p port] [-d datafile] [-k ping_s] [-H host]... [-o origin]... [-v] [-V] [-U]\n"
        "  -k  websocket ping interval in seconds (default %d, 0 = off); silent peers are\n"
        "      closed after %d intervals\n"
        "  -H  extra Host name to accept besides IP literals, localhost, the hostname and\n"
        "      hostname.local (repeatable; '*' accepts any Host)\n"
        "  -o  extra browser Origin allowed to write and open websockets, e.g.\n"
        "      http://printer.lan (repeatable)\n", argv0, WS_PING_INTERVAL_S, WS_TIMEOUT_PINGS);
}

int main(int argc, char **argv) {
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-l") == 0 && i + 1 < argc) g_listen_addr = argv[++i];
        else if (strcmp(argv[i], "-p") == 0 && i + 1 < argc) g_listen_port = atoi(argv[++i]);
        else if (strcmp(argv[i], "-d") == 0 && i + 1 < argc) g_data_path = argv[++i];
        else if (strcmp(argv[i], "-k") == 0 && i + 1 < argc) g_ws_ping_interval = atoi(argv[++i]);
        else if (strcmp(argv[i], "-H") == 0 && i + 1 < argc) {
            const char *h = argv[++i];
            if (strcmp(h, "*") == 0) g_allow_any_host = 1;
            else if (g_allow_hosts_n < MAX_ALLOW_NAMES) g_allow_hosts[g_allow_hosts_n++] = h;
            else { fprintf(stderr, "microspool: too many -H names (max %d)\n", MAX_ALLOW_NAMES); return 1; }
        }
        else if (strcmp(argv[i], "-o") == 0 && i + 1 < argc) {
            if (g_allow_origins_n < MAX_ALLOW_NAMES) g_allow_origins[g_allow_origins_n++] = argv[++i];
            else { fprintf(stderr, "microspool: too many -o origins (max %d)\n", MAX_ALLOW_NAMES); return 1; }
        }
        else if (strcmp(argv[i], "-v") == 0) g_verbose = 1;
        else if (strcmp(argv[i], "-U") == 0) g_ui_enabled = 0;
        else if (strcmp(argv[i], "-V") == 0 || strcmp(argv[i], "--version") == 0) {
            printf("microspool %s (Spoolman API %s)\n", MICROSPOOL_VERSION, SERVER_VERSION);
            return 0;
        }
        else { print_usage(argv[0]); return 1; }
    }

    if (gethostname(g_hostname, sizeof g_hostname - 1) != 0) g_hostname[0] = 0;
    g_hostname[sizeof g_hostname - 1] = 0;
    str_lower(g_hostname);

    if (load_data() != 0) {
        fprintf(stderr, "microspool: файл данных повреждён, выхожу без изменений\n");
        return 1;
    }
    g_last_use_flush_mono = mono_now();

    struct sigaction sa_term;
    memset(&sa_term, 0, sizeof sa_term);
    sa_term.sa_handler = on_term_signal;
    sigaction(SIGTERM, &sa_term, NULL);
    sigaction(SIGINT, &sa_term, NULL);
    signal(SIGPIPE, SIG_IGN);

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) { perror("socket"); return 1; }
    int opt = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof opt);
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)g_listen_port);
    if (inet_pton(AF_INET, g_listen_addr, &addr.sin_addr) != 1) {
        fprintf(stderr, "microspool: неверный адрес прослушивания: %s\n", g_listen_addr);
        return 1;
    }
    if (bind(lfd, (struct sockaddr *)&addr, sizeof addr) != 0) { perror("bind"); return 1; }
    if (listen(lfd, LISTEN_BACKLOG) != 0) { perror("listen"); return 1; }
    fcntl(lfd, F_SETFL, O_NONBLOCK);

    for (int i = 0; i < MAX_CONNS; i++) { g_conns[i].fd = -1; g_conns[i].in_use = 0; }

    log_v("слушаем %s:%d, данные %s", g_listen_addr, g_listen_port, g_data_path);

    while (!g_should_exit) {
        struct pollfd pfds[1 + MAX_CONNS];
        Conn *conn_for_pfd[1 + MAX_CONNS];
        int nfds = 0;
        pfds[nfds].fd = lfd; pfds[nfds].events = POLLIN; conn_for_pfd[nfds] = NULL; nfds++;
        for (int i = 0; i < MAX_CONNS; i++) {
            if (!g_conns[i].in_use) continue;
            short events = g_conns[i].rd_eof ? 0 : POLLIN;
            if (g_conns[i].tx_len > g_conns[i].tx_sent) events |= POLLOUT;
            pfds[nfds].fd = g_conns[i].fd;
            pfds[nfds].events = events;
            pfds[nfds].revents = 0;
            conn_for_pfd[nfds] = &g_conns[i];
            nfds++;
        }

        double now = mono_now();
        double next_deadline = now + 5.0;
        for (int i = 0; i < MAX_CONNS; i++) {
            double d = conn_next_deadline(&g_conns[i]);
            if (d < next_deadline) next_deadline = d;
        }
        if (g_use_pending && g_use_flush_deadline < next_deadline) next_deadline = g_use_flush_deadline;
        double timeout_s = next_deadline - now;
        if (timeout_s < 0) timeout_s = 0;
        int timeout_ms = (int)(timeout_s * 1000.0) + 1;

        int pr = poll(pfds, (nfds_t)nfds, timeout_ms);
        if (pr < 0) {
            if (errno == EINTR) continue;
            perror("poll");
            break;
        }

        if (pfds[0].revents & POLLIN) accept_clients(lfd);

        for (int k = 1; k < nfds; k++) {
            Conn *c = conn_for_pfd[k];
            if (!c || !c->in_use || c->fd != pfds[k].fd) continue;
            if (pfds[k].revents & (POLLHUP | POLLERR | POLLNVAL)) { conn_reset(c); continue; }
            if (pfds[k].revents & POLLIN) {
                conn_on_readable(c);
                if (!c->in_use) continue;
            }
            if (pfds[k].revents & POLLOUT) {
                conn_on_writable(c);
                if (!c->in_use) continue;
            }
        }

        now = mono_now();
        for (int i = 0; i < MAX_CONNS; i++) conn_run_timers(&g_conns[i], now);

        if (g_use_pending && now >= g_use_flush_deadline) save_data();
    }

    save_data();
    log_v("получен сигнал завершения, данные сохранены");
    for (int i = 0; i < MAX_CONNS; i++) if (g_conns[i].in_use) conn_reset(&g_conns[i]);
    close(lfd);
    return 0;
}
