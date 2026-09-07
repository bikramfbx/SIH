#!/usr/bin/env bash
# Disposable offline fresh-install test stack.
#
#   * reuses the already-built sih-api image (tagged sih-fresh-api:latest)
#   * spins an isolated compose project "sih-fresh" with its own DB volume
#   * FIRMS is a local mock (tests/fresh/mock_firms_server.py)
#   * runs one worker cycle, then exercises the API (data, 422s, frontend,
#     health/degraded, DB-down 503) and failure modes
#   * ALWAYS tears the stack down (down -v)
#
# Usage: bash tests/fresh_install.sh
dir_here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJ=sih-fresh
RF="$dir_here/fresh/compose.fresh.yml"
cd "$dir_here" || exit 2
CHECKS=0
FAILS=0

pass() { CHECKS=$((CHECKS+1)); printf 'PASS  %s\n' "$*"; }
fail() { CHECKS=$((CHECKS+1)); FAILS=$((FAILS+1)); printf 'FAIL  %s\n' "$*"; }

docker tag sih-api:latest sih-fresh-api:latest >/dev/null 2>&1 \
    || { echo "ERROR: sih-api:latest image not found"; exit 2; }

cleanup() {
    docker compose -p "$PROJ" -f "$RF" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "== starting disposable stack (project=$PROJ) =="
docker compose -p "$PROJ" -f "$RF" up -d 2>&1 | tail -5

echo "== waiting for db + api =="
for i in $(seq 1 60); do
    if docker exec sih-fresh-db-1 pg_isready -U thermal_admin -d thermal_anomaly >/dev/null 2>&1; then
        break
    fi
    sleep 2
done
for i in $(seq 1 60); do
    code=$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8101/api/health 2>/dev/null)
    [ "$code" = "200" ] && break
    sleep 2
done
[ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8101/api/health)" = "200" ] \
    && pass "db+api reachable" || fail "db+api reachable"

echo "== worker: first cycle (empty DB) =="
docker compose -p "$PROJ" -f "$RF" exec -T worker python scripts/worker.py --once >/tmp/fresh_worker1.log 2>&1
grep -q "total_new\|inserted=" /tmp/fresh_worker1.log || true
tail -3 /tmp/fresh_worker1.log

H() { curl -s http://127.0.0.1:8101/api/health; }

inserted=$(H | python3 -c 'import sys,json;print(json.load(sys.stdin).get("inserted_24h",0))')
[ "$inserted" = "4" ] && pass "worker ingested 4 hotspots (inserted_24h=$inserted)" \
    || fail "worker ingested 4 hotspots (got $inserted)"

total=$(H | python3 -c 'import sys,json;print(json.load(sys.stdin).get("total_hotspots",0))')
[ "$total" = "4" ] && pass "total_hotspots=4" || fail "total_hotspots=4 (got $total)"

classified=$(H | python3 -c 'import sys,json;print(json.load(sys.stdin).get("classified_count",0))')
[ "$classified" = "4" ] && pass "enrichment+classification ran for 4 (=classified_count $classified)" \
    || fail "classified_count=4 (got $classified)"

srcs=$(H | python3 -c 'import sys,json;d=json.load(sys.stdin);print(",".join(sorted(d.get("sources_used",[]) or [])))')
[ "$srcs" = "VIIRS_NOAA20_NRT,VIIRS_NOAA21_NRT" ] && pass "per-source run log present ($srcs)" \
    || fail "per-source run log present (got $srcs)"

worker_up=$(H | python3 -c 'import sys,json;print(json.load(sys.stdin).get("worker",{}).get("alive"))')
[ "$worker_up" = "True" ] && pass "worker heartbeat recorded" || fail "worker heartbeat recorded (got $worker_up)"

echo "== API: data + validation =="
n=$(curl -s 'http://127.0.0.1:8101/api/hotspots.geojson?bbox=68,6,98,38&limit=100' \
    | python3 -c 'import sys,json;print(len(json.load(sys.stdin).get("features",[])))')
[ "$n" = "4" ] && pass "geojson returns 4 features" || fail "geojson returns 4 features (got $n)"

c=$(curl -s -o /dev/null -w '%{http_code}' 'http://127.0.0.1:8101/api/hotspots.geojson?bbox=999,1,2,3')
[ "$c" = "422" ] && pass "bad bbox -> 422" || fail "bad bbox -> 422 (got $c)"

c=$(curl -s -o /dev/null -w '%{http_code}' 'http://127.0.0.1:8101/api/hotspots.geojson?date_from=2026-13-01')
[ "$c" = "422" ] && pass "invalid date -> 422" || fail "invalid date -> 422 (got $c)"

c=$(curl -s -o /dev/null -w '%{http_code}' 'http://127.0.0.1:8101/api/hotspots.geojson?date_from=2026-09-08&date_to=2026-09-01')
[ "$c" = "422" ] && pass "date_from>date_to -> 422" || fail "date_from>date_to -> 422 (got $c)"

c=$(curl -s -o /dev/null -w '%{http_code}' 'http://127.0.0.1:8100/' -u '' 2>/dev/null)
fe=$(curl -s http://127.0.0.1:8101/ | grep -c 'leaflet\|hotspot' || true)
[ "$fe" -ge 1 ] && pass "frontend served by api" || fail "frontend served by api"

echo "== failure modes =="
echo "-- idempotent rerun (same data) =="
docker compose -p "$PROJ" -f "$RF" exec -T worker python scripts/worker.py --once >/tmp/fresh_worker2.log 2>&1
dup=$(H | python3 -c 'import sys,json;print(json.load(sys.stdin).get("inserted_24h",0))')
[ "$dup" = "4" ] && pass "idempotent rerun: no extra rows (inserted_24h stays 4)" \
    || fail "idempotent rerun (inserted_24h=$dup)"

echo "-- FIRMS outage (mock returns 500) =="
MOCK_MODE=fail500 docker compose -p "$PROJ" -f "$RF" up -d --force-recreate --no-deps mockfirms 2>&1 | tail -1
docker compose -p "$PROJ" -f "$RF" exec -T worker python scripts/worker.py --once >/tmp/fresh_worker3.log 2>&1
st=$(H | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("last_run",{}).get("status",""))')
[ "$st" = "failed" ] && pass "FIRMS outage recorded as failed run (not crash)" \
    || fail "FIRMS outage recorded (last_run.status=$st)"
pipeline_error=$(H | python3 -c 'import sys,json;print("pipeline_error" in json.load(sys.stdin))')
[ "$pipeline_error" = "True" ] && pass "health degrades with pipeline_error" \
    || fail "health degrades with pipeline_error"
MOCK_MODE=ok docker compose -p "$PROJ" -f "$RF" up -d --force-recreate --no-deps mockfirms 2>&1 | tail -1

echo "-- DB down -> health 503 =="
docker compose -p "$PROJ" -f "$RF" stop db >/dev/null 2>&1
sleep 1
c=$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8101/api/health)
[ "$c" = "503" ] && pass "db down -> health 503" || fail "db down -> health 503 (got $c)"
docker compose -p "$PROJ" -f "$RF" start db >/dev/null 2>&1

echo
echo "== summary: $CHECKS checks, $FAILS failures =="
[ "$FAILS" = "0" ] && echo "FRESH-INSTALL TEST: PASS" || echo "FRESH-INSTALL TEST: FAIL"
exit $FAILS