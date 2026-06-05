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

## Krok 6: Uruchomienie API oraz konsumenta

Upewnij się, że w środowisku znajduje się plik model.pkl. Jeśli chcesz wytrenować nowy model, przejdź do sekcji wskazówki.
W dwóch osobnych terminalach uruchom API oraz consumer predict

### Terminal C (API)

```bash
python api.py
```

### Terminal D (consumer_predict)

```bash
python consumer_predict.py
```

W terminalu consumer_predict, co minutę zobaczysz komunikat informujący czy zamknięta świeca była anomalią, czy nie. Anomalie wysyłane są do tematu alerts i można je "podejrzeć" za pomocą "view_alerts.ipynb". Jeśli chcesz sztucznie wywołać anomalię, użyj pliku "test_inject.ipynb".

## Wskazówki

### Logi

W terminalu producenta zobaczysz logi przesyłanych komunikatów.

### Baza danych

Plik `rta.db` zostanie automatycznie utworzony w katalogu, z którego uruchamiasz skrypty, w momencie pierwszego zapisu batcha.

### Stabilność

Z-score wymaga zebrania historii (okno 10 minut), dlatego pełną precyzję statystyczną osiągnie po krótkim czasie od uruchomienia.

### Trenowanie modelu

Należy wykonać kroki 1-5. Programy producer_binance.py, spark_vwap.py oraz spark_zscore.py należy zostawić włączone na długi okres czasu (ok. 2h). W tym czasie zostanie zebrana wystarczająca ilość i zróżnicowanie danych aby model mógł się precyzyjnie wytrenować. Następnie należy zatrzymać wszsytkie procesy i uruchomić program train_model.py. Stworzy on plik model.pkl używany do wykrywania anomalii.

Jeśli model został wcześniej wytrenowany, można użyć go ponownie w przyszłości i pominąć ten krok.