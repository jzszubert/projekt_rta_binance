# Projekt RTA - Analiza Strumieniowa Binance

Projekt realizuje przetwarzanie w czasie rzeczywistym danych z giełdy Binance (transakcje oraz świece 1-minutowe) przy użyciu Apache Kafka oraz PySpark Structured Streaming. Wyniki analizy (wskaźnik VWAP oraz detekcja anomalii wolumenu Z-score) są zapisywane do lokalnej bazy danych SQLite.

## Architektura

* **Źródło:** Binance WebSocket API
* **Broker:** Apache Kafka (tematy: `trades`, `klines`, `alerts`)
* **Przetwarzanie:** PySpark (wersja 3.5.0 / 4.0.0-preview2)
* **Baza docelowa:** SQLite (`rta.db`)

## Instrukcja uruchomienia (Czysty start)

Aby uniknąć błędów związanych z niekompatybilnością schematów po modyfikacjach kodu, zaleca się uruchamianie środowiska od nowa, usuwając historyczne punkty przywracania.

### Krok 1: Zatrzymanie obecnego środowiska

W głównym katalogu projektu wyłącz działające kontenery:

```bash
docker compose down