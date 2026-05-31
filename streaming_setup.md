# Projekt RTA – Instrukcja uruchomienia

Aby uruchomić środowisko od zera i uniknąć konfliktów danych, wykonaj poniższe kroki w podanej kolejności.

## Krok 1: Zatrzymanie obecnego środowiska

W głównym katalogu projektu wyłącz działające kontenery:

```bash
docker compose down
```

## Krok 2: Usunięcie starych stanów i bazy danych

Usuń pliki checkpointów Sparka oraz stary plik bazy danych, aby system zaczął rejestrować historię od zera:

```bash
rm -rf /tmp/checkpoints/zscore
rm -rf /tmp/checkpoints/vwap_1min
rm -rf /tmp/checkpoints/vwap_5min
rm -f rta.db
```

## Krok 3: Uruchomienie infrastruktury

Uruchom usługi (m.in. Kafka) w tle:

```bash
docker compose up -d
```

## Krok 4: Uruchomienie producenta danych

W jednym oknie terminala uruchom skrypt integrujący oba strumienie danych (`trades` oraz `klines`):

```bash
python producer_binance.py
```

## Krok 5: Uruchomienie analizy PySpark

W dwóch osobnych terminalach uruchom aplikacje analityczne.

### Terminal A (VWAP)

```bash
python spark_vwap.py
```

### Terminal B (Z-score)

```bash
python spark_zscore.py
```

## Wskazówki

### Logi

W terminalu producenta zobaczysz logi przesyłanych komunikatów.

### Baza danych

Plik `rta.db` zostanie automatycznie utworzony w katalogu, z którego uruchamiasz skrypty, w momencie pierwszego zapisu batcha.

### Stabilność

Z-score wymaga zebrania historii (okno 10 minut), dlatego pełną precyzję statystyczną osiągnie po krótkim czasie od uruchomienia.
