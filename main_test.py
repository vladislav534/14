import asyncio
import websockets
import json
import logging
import ssl
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Optional, Tuple
from collections import defaultdict
import os
import gzip
import csv
import pandas as pd
import locale
import random
import time
from decimal import Decimal
import aiohttp


# ==================== ВЫСОКОЧАСТОТНАЯ КОНФИГУРАЦИЯ ====================

class Config:
    """ВЫСОКОЧАСТОТНАЯ КОНФИГУРАЦИЯ ДЛЯ ФЬЮЧЕРСНОГО АРБИТРАЖА"""
    
    # Список бирж для подключения (приоритет по скорости)
    EXCHANGES = [
        'binance', 'bybit', 'okx', 'gateio', 'bitget', 
        'htx', 'kraken', 'bingx', 'mexc' #'bitfinex', 'dydx', 'kucoin', #'phemex'
    ]
    
    # Основные торговые пары фьючерсов
    FUTURES_SYMBOLS = [
        'BTCUSDT', 'ETHUSDT', 'SUIUSDT', 'SOLUSDT',
        'XRPUSDT', 'DOGEUSDT', 'BNBUSDT', 'AVAXUSDT',
        'DOGEUSDT', 'TAOUSDT', 'LTCUSDT', 'ADAUSDT'
    ]
    
    # АГРЕССИВНЫЕ пороги для высокочастотного арбитража
    OPEN_SPREAD_THRESHOLD = 0.60    # Уменьшенный порог открытия
    CLOSE_SPREAD_THRESHOLD = 0.05   # Уменьшенный порог закрытия
    MAX_OPEN_TRADES = 15            # Увеличен лимит сделок
    MAX_TRADES_PER_EXCHANGE = 2  # Максимум 2 сделки на одной бирже
    MAX_TRADES_PER_SYMBOL = 15  
    
    # Оптимизированные таймауты для высокой частоты
    PRICE_MAX_AGE = 1.0             # Уменьшен максимальный возраст цены
    RECONNECT_INTERVAL = 2          # Быстрое переподключение
    HEARTBEAT_INTERVAL = 15         # Учащенный heartbeat
    
    # Уменьшенные комиссии для агрессивного трейдинга
    FUTURES_FEES = {
        'binance': 0.0005, 'bybit': 0.00055, 'okx': 0.0005, 
        'gateio': 0.0005, 'bitget': 0.001, 'htx': 0.0006,
        'kraken': 0.0005, 'bingx': 0.0005, 'mexc': 0.0002,
        'kucoin': 0.0006, 'phemex': 0.0006, 'lbank':0.0006,
        'bitfinex': 0.00065, 'dydx': 0.0005
    }

# ==================== ОПТИМИЗИРОВАННОЕ ЛОГГИРОВАНИЕ ====================

def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'arbitrage_bot_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logger()

# ==================== ВЫСОКОЧАСТОТНЫЙ ОБРАБОТЧИК ЦЕН ====================

class HighFrequencyPriceHandler:
    def __init__(self):
        self.prices = defaultdict(dict)
        self.timestamps = defaultdict(lambda: defaultdict(float))
        self.last_update_time = time.time()
        self.update_count = 0
        self.exchange_updates = defaultdict(int)
        self.connection_status = defaultdict(bool)
        self._price_cache = {}
        self._cache_ttl = 0.05  # Кэш на 50ms для максимальной скорости
        
    async def handle_price_update(self, exchange: str, symbol: str, price: float):
        """Сверхбыстрая обработка обновления цены"""
        current_time = time.time()
        
        # Мгновенное обновление без лишних проверок
        self.prices[symbol][exchange] = price
        self.timestamps[symbol][exchange] = current_time
        self.exchange_updates[exchange] += 1
        self.connection_status[exchange] = True
        self.update_count += 1
        self.last_update_time = current_time
        
        # Мгновенная инвалидация кэша
        if symbol in self._price_cache:
            del self._price_cache[symbol]

    def get_current_prices(self, symbol: str) -> Dict[str, float]:
        """Оптимизированное получение цен с кэшированием"""
        current_time = time.time()
        
        # Проверяем кэш для максимальной скорости
        cache_key = symbol
        if cache_key in self._price_cache:
            cache_data, cache_time = self._price_cache[cache_key]
            if current_time - cache_time < self._cache_ttl:
                return cache_data
        
        # Быстрое получение свежих цен
        fresh_prices = {}
        symbol_prices = self.prices.get(symbol, {})
        symbol_timestamps = self.timestamps.get(symbol, {})
        
        for exchange, price in symbol_prices.items():
            timestamp = symbol_timestamps.get(exchange, 0)
            if current_time - timestamp <= Config.PRICE_MAX_AGE:
                fresh_prices[exchange] = price
        
        # Кэшируем результат на 50ms
        self._price_cache[cache_key] = (fresh_prices.copy(), current_time)
        return fresh_prices

    def is_price_fresh(self, exchange: str, symbol: str) -> bool:
        """Сверхбыстрая проверка свежести цены"""
        timestamp = self.timestamps[symbol].get(exchange, 0)
        return time.time() - timestamp <= Config.PRICE_MAX_AGE

    def get_active_exchanges_for_symbol(self, symbol: str) -> List[str]:
        """Быстрое получение активных бирж для символа"""
        current_time = time.time()
        active = []
        symbol_timestamps = self.timestamps.get(symbol, {})
        
        for exchange in Config.EXCHANGES:
            timestamp = symbol_timestamps.get(exchange, 0)
            if current_time - timestamp <= Config.PRICE_MAX_AGE:
                active.append(exchange)
        
        return active

    def get_exchange_stats(self) -> Dict[str, int]:
        """Быстрая статистика по биржам"""
        stats = {}
        current_time = time.time()
        
        for exchange in Config.EXCHANGES:
            active_symbols = 0
            for symbol in Config.FUTURES_SYMBOLS:
                timestamp = self.timestamps[symbol].get(exchange, 0)
                if current_time - timestamp <= Config.PRICE_MAX_AGE:
                    active_symbols += 1
            stats[exchange] = active_symbols
        
        return stats



# ==================== WEB SOCKET МЕНЕДЖЕР ====================

class WebSocketManager:
    def __init__(self, price_handler: HighFrequencyPriceHandler):
        self.price_handler = price_handler
        self.connections = {}
        self.is_running = False
        self.connected_exchanges = set()
        self.failed_exchanges = set()
        self.connection_tasks = {}
        self.last_reconnect_attempt = defaultdict(float)
        
    async def start(self):
        """Запуск всех WebSocket соединений с улучшенной логикой"""
        self.is_running = True
        logger.info("🚀 Starting WebSocket connections for futures...")
        
        # Запускаем подключение к каждой бирже в отдельной задаче
        tasks = []
        for exchange in Config.EXCHANGES:
            task = asyncio.create_task(self.manage_exchange_connection(exchange))
            tasks.append(task)
            self.connection_tasks[exchange] = task
        
        # Не ждем завершения, продолжаем работу
        asyncio.create_task(self.monitor_connections())
        asyncio.create_task(self.monitor_connection_health())
        logger.info("✅ WebSocket manager started")

    async def manage_exchange_connection(self, exchange: str):
        """Управление подключением к конкретной бирже с улучшенной обработкой ошибок"""
        logger.info(f"🔧 Starting connection manager for {exchange}")
        
        retry_count = 0
        max_retries = 8  # Увеличиваем количество попыток
        
        while self.is_running and retry_count < max_retries:
            try:
                logger.info(f"🔄 Connecting to {exchange} (attempt {retry_count + 1})...")
                
                # Закрываем предыдущее соединение если есть
                if exchange in self.connections and self.connections[exchange]:
                    try:
                        await self.connections[exchange].close()
                    except:
                        pass
                
                if await self.connect_exchange_single(exchange):
                    self.connected_exchanges.add(exchange)
                    self.failed_exchanges.discard(exchange)
                    logger.info(f"✅ Successfully connected to {exchange}")
                    retry_count = 0  # Сбрасываем счетчик при успешном подключении
                    
                    # Ждем пока соединение не разорвется
                    await self.wait_for_connection_close(exchange)
                    
                else:
                    retry_count += 1
                    logger.warning(f"❌ Connection attempt {retry_count} failed for {exchange}")
                    
            except Exception as e:
                logger.error(f"❌ Connection failed for {exchange}: {e}")
                retry_count += 1
            
            if retry_count > 0 and retry_count < max_retries:
                wait_time = min(60, 5 * (2 ** retry_count))  # Уменьшаем максимальное время ожидания
                logger.info(f"⏰ Waiting {wait_time}s before reconnecting to {exchange}")
                await asyncio.sleep(wait_time)
        
        if retry_count >= max_retries:
            logger.error(f"❌ Max retries reached for {exchange}, marking as failed")
            self.failed_exchanges.add(exchange)
            self.connected_exchanges.discard(exchange)
    def is_connection_open(self, websocket) -> bool:
        """Универсальная и безопасная проверка состояния соединения"""
        try:
            if websocket is None:
                return False
                
            # Для стандартных websockets
            if hasattr(websocket, 'closed'):
                if callable(websocket.closed):
                    return not websocket.closed()
                else:
                    return not websocket.closed
            
            # Для aiohttp ClientWebSocketResponse
            if hasattr(websocket, 'closed') and not callable(websocket.closed):
                return not websocket.closed
                
            # Для объектов с состоянием
            if hasattr(websocket, 'state'):
                state = websocket.state
                if hasattr(state, 'name'):  # Для enum состояний
                    state = state.name
                open_states = {'OPEN', 'CONNECTED', 'CONNECTING'}
                return state in open_states
                
            # Если не можем определить - считаем открытым и полагаемся на исключения
            return True
            
        except Exception as e:
            logger.debug(f"Connection check error: {e}")
            return False
    async def connect_exchange_single(self, exchange: str) -> bool:
        """Одна попытка подключения к бирже"""
        try:
            if exchange == 'binance':
                return await self.connect_binance()
            elif exchange == 'bybit':
                return await self.connect_bybit()
            elif exchange == 'okx':
                return await self.connect_okx()
            elif exchange == 'kraken':
                return await self.connect_kraken()
            elif exchange == 'htx':
                return await self.connect_htx()
            elif exchange == 'gateio':
                return await self.connect_gateio()
            elif exchange == 'bitget':
                return await self.connect_bitget()
            elif exchange == 'mexc':
                return await self.connect_mexc()
            elif exchange == 'bingx':
                return await self.connect_bingx()
            elif exchange == 'kucoin':
                return await self.connect_kucoin()
            elif exchange == 'phemex':
                return await self.connect_phemex()
            elif exchange == 'coinbase':
                return await self.connect_coinbase()
            elif exchange == 'bitfinex':
                return await self.connect_bitfinex()
            elif exchange == 'dydx':
                return await self.connect_dydx()
            else:
                logger.warning(f"Unknown exchange: {exchange}")
                return False
                
        except Exception as e:
            logger.error(f"Connection error for {exchange}: {e}")
            return False

    async def wait_for_connection_close(self, exchange: str):
        """Ожидание разрыва соединения"""
        try:
            if exchange in self.connections:
                await self.connections[exchange].wait_closed()
        except Exception as e:
            logger.debug(f"Connection close wait for {exchange}: {e}")
        finally:
            if exchange in self.connections:
                self.connections[exchange] = None
            self.connected_exchanges.discard(exchange)
            logger.warning(f"🔌 Connection lost for {exchange}")

    async def monitor_connections(self):
        """Мониторинг состояния всех соединений с улучшенной диагностикой"""
        while self.is_running:
            try:
                await asyncio.sleep(30)
                
                current_stats = self.price_handler.get_exchange_stats()
                current_time = time.time()
                logger.info("📊 ДЕТАЛЬНЫЙ СТАТУС ПОДКЛЮЧЕНИЙ:")
                
                problem_exchanges = []
                healthy_exchanges = []
                
                for exchange in Config.EXCHANGES:
                    symbols_count = current_stats.get(exchange, 0)
                    updates_count = self.price_handler.exchange_updates.get(exchange, 0)
                    is_connected = exchange in self.connected_exchanges
                    is_failed = exchange in self.failed_exchanges
                    has_websocket = exchange in self.connections and self.connections[exchange] is not None
                    websocket_open = has_websocket and self.is_connection_open(self.connections[exchange])
                    
                    # Определяем статус
                    if symbols_count > 0 and is_connected and websocket_open:
                        status = "✅"
                        healthy_exchanges.append(exchange)
                    elif is_connected and (symbols_count == 0 or not websocket_open):
                        status = "⚠️" 
                        problem_exchanges.append(exchange)
                        # АВТОМАТИЧЕСКОЕ ПЕРЕПОДКЛЮЧЕНИЕ ПРОБЛЕМНЫХ БИРЖ
                        logger.warning(f"🔄 Автоматическое переподключение проблемной биржи: {exchange}")
                        await self.reconnect_single_exchange(exchange)
                    elif is_failed:
                        status = "❌"
                        problem_exchanges.append(exchange)
                    else:
                        status = "🔄"
                        problem_exchanges.append(exchange)
                    
                    logger.info(f"  {status} {exchange}: "
                            f"{symbols_count:2d} symbols, "
                            f"{updates_count:6d} updates, "
                            f"WS: {'open' if websocket_open else 'closed'}, "
                            f"Conn: {'yes' if is_connected else 'no'}, "
                            f"Fail: {'yes' if is_failed else 'no'}")
                
                # Логируем сводку
                logger.info(f"📈 СВОДКА: {len(healthy_exchanges)} здоровых, "
                        f"{len(problem_exchanges)} проблемных бирж")
                
                if problem_exchanges:
                    logger.warning(f"🔧 Проблемные биржи: {', '.join(problem_exchanges)}")
                
                # Автоматически переподключаем проблемные биржи
                if problem_exchanges:
                    await self.check_and_reconnect_failed_exchanges()
                            
            except Exception as e:
                logger.error(f"Connection monitor error: {e}")

    # ==================== РЕАЛИЗАЦИИ ПОДКЛЮЧЕНИЙ ДЛЯ КАЖДОЙ БИРЖИ ====================
    async def connect_dydx(self) -> bool:
        """dYdX упрощенная версия"""
        try:
            # Пробуем основной endpoint
            url = "wss://api.dydx.exchange/v3/ws"
            
            logger.info(f"🔄 dYdX: Connecting to {url}")
            
            websocket = await websockets.connect(
                url,
                ping_interval=30,
                ping_timeout=20,
                close_timeout=15
            )
            self.connections['dydx'] = websocket
            
            # ТОЛЬКО BTC ДЛЯ НАЧАЛА
            subscribe_msg = {
                "type": "subscribe",
                "channel": "v3_ticker",
                "id": "BTC-USD"
            }
            
            await websocket.send(json.dumps(subscribe_msg))
            logger.info("✅ dYdX subscribed to BTC-USD")
            
            asyncio.create_task(self.handle_dydx_messages(websocket))
            return True
            
        except Exception as e:
            logger.error(f"❌ dYdX connection failed: {e}")
            return False

    async def handle_dydx_messages(self, websocket):
        """Упрощенный обработчик dYdX"""
        logger.info("📝 dYdX handler started")
        
        while self.is_running and self.is_connection_open(websocket):
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=30)
                data = json.loads(message)
                
                # Обработка тикера
                if data.get('type') == 'channel_data' and data.get('channel') == 'v3_ticker':
                    contents = data.get('contents', {})
                    if 'price' in contents:
                        price = float(contents['price'])
                        await self.price_handler.handle_price_update('dydx', 'BTCUSDT', price)
                        
                # Обработка подписки
                elif data.get('type') == 'subscribed':
                    logger.info(f"✅ dYdX subscription confirmed: {data}")
                    
            except asyncio.TimeoutError:
                try:
                    if self.is_connection_open(websocket):
                        ping_msg = {"type": "ping"}
                        await websocket.send(json.dumps(ping_msg))
                except:
                    break
            except Exception as e:
                logger.error(f"❌ dYdX message error: {e}")
                break
        
        logger.info("🔚 dYdX handler stopped")
    async def connect_bitfinex(self) -> bool:
        """Bitfinex упрощенная версия"""
        try:
            url = "wss://api-pub.bitfinex.com/ws/2"
            
            logger.info(f"🔄 Bitfinex: Connecting to {url}")
            
            websocket = await websockets.connect(
                url,
                ping_interval=25,
                ping_timeout=15,
                close_timeout=10
            )
            self.connections['bitfinex'] = websocket
            
            # ТОЛЬКО BTC И ETH ДЛЯ НАЧАЛА
            bitfinex_symbols = ["tBTCUST", "tETHUST"]
            
            for symbol_bfx in bitfinex_symbols:
                subscribe_msg = {
                    "event": "subscribe",
                    "channel": "ticker",
                    "symbol": symbol_bfx
                }
                
                await websocket.send(json.dumps(subscribe_msg))
                logger.info(f"📨 Bitfinex subscribing to: {symbol_bfx}")
                await asyncio.sleep(0.3)
            
            logger.info("✅ Bitfinex subscriptions sent")
            
            asyncio.create_task(self.handle_bitfinex_messages(websocket))
            return True
            
        except Exception as e:
            logger.error(f"❌ Bitfinex connection failed: {e}")
            return False

    async def handle_bitfinex_messages(self, websocket):
        """Упрощенный обработчик Bitfinex"""
        logger.info("📝 Bitfinex handler started")
        
        symbol_mapping = {}
        
        while self.is_running and self.is_connection_open(websocket):
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=20)
                data = json.loads(message)
                
                # Обработка ping
                if data == {"event": "ping"}:
                    await websocket.send(json.dumps({"event": "pong"}))
                    continue
                    
                # Обработка подписки
                if isinstance(data, dict) and data.get('event') == 'subscribed':
                    channel_id = data['chanId']
                    symbol_bfx = data['symbol']
                    
                    # Простой маппинг
                    if symbol_bfx == "tBTCUST":
                        symbol = "BTCUSDT"
                    elif symbol_bfx == "tETHUST":
                        symbol = "ETHUSDT"
                    else:
                        symbol = symbol_bfx.replace('t', '').replace('UST', 'USDT')
                    
                    symbol_mapping[channel_id] = symbol
                    logger.info(f"✅ Bitfinex subscribed to {symbol_bfx} -> {symbol}")
                    continue
                    
                # Обработка тикера
                if isinstance(data, list) and len(data) > 1:
                    channel_id = data[0]
                    ticker_data = data[1]
                    
                    if channel_id in symbol_mapping and isinstance(ticker_data, list):
                        symbol = symbol_mapping[channel_id]
                        
                        # Bitfinex ticker format: [BID, BID_SIZE, ASK, ASK_SIZE, DAILY_CHANGE, DAILY_CHANGE_RELATIVE, LAST_PRICE, VOLUME, HIGH, LOW]
                        if len(ticker_data) >= 7 and ticker_data[6]:
                            price = float(ticker_data[6])
                            await self.price_handler.handle_price_update('bitfinex', symbol, price)
                
                # Периодический ping
                ping_msg = {"event": "ping", "cid": int(time.time())}
                await websocket.send(json.dumps(ping_msg))
                            
            except asyncio.TimeoutError:
                # Ping при таймауте
                try:
                    if self.is_connection_open(websocket):
                        ping_msg = {"event": "ping", "cid": int(time.time())}
                        await websocket.send(json.dumps(ping_msg))
                except:
                    break
            except Exception as e:
                logger.error(f"❌ Bitfinex message error: {e}")
                break
        
        logger.info("🔚 Bitfinex handler stopped")
    async def connect_coinbase(self) -> bool:
            """Coinbase: Безопасная подписка (совместимо с VPN)"""
            try:
                # Закрываем старое соединение если есть
                if 'coinbase' in self.connections and self.connections['coinbase']:
                    try:
                        await self.connections['coinbase'].close()
                    except:
                        pass

                url = "wss://ws-feed.exchange.coinbase.com"
                logger.info(f"🔄 Coinbase: Connecting to {url}")
                
                # Убираем пинги, так как Coinbase шлет heartbeat
                websocket = await websockets.connect(
                    url,
                    ping_interval=None,
                    max_size=None,
                    close_timeout=10
                )
                self.connections['coinbase'] = websocket
                
                # СПИСОК СИМВОЛОВ (Сокращенный для стабильности)
                coinbase_symbols = ["BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD"]
                
                logger.info("⏳ Coinbase: Starting SLOW subscription...")
                
                for i, symbol in enumerate(coinbase_symbols):
                    if not self.is_connection_open(websocket):
                        logger.error("❌ Coinbase: Connection closed during subscription")
                        return False

                    subscribe_message = {
                        "type": "subscribe",
                        "product_ids": [symbol],
                        "channels": ["ticker"]
                    }
                    
                    await websocket.send(json.dumps(subscribe_message))
                    logger.info(f"📨 Coinbase: Subscribed to {symbol}")
                    
                    # !!! ВАЖНО: ЗАДЕРЖКА 1.5 СЕКУНДЫ !!!
                    # Это предотвращает бан IP вашего VPN
                    await asyncio.sleep(1.5)
                
                asyncio.create_task(self.handle_coinbase_messages_improved(websocket))
                return True
                
            except Exception as e:
                logger.error(f"❌ Coinbase connection failed: {e}")
                return False

    async def handle_coinbase_messages_improved(self, websocket):
        """Улучшенный обработчик Coinbase"""
        logger.info("📝 Coinbase improved handler started")
        
        while self.is_running and self.is_connection_open(websocket):
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=30)
                data = json.loads(message)
                
                # Логируем первые несколько сообщений для диагностики
                # logger.info(f"🔍 Coinbase message: {data}")
                
                # Обработка тикера
                if data.get('type') == 'ticker' and 'product_id' in data:
                    symbol_cb = data['product_id']  # Формат "BTC-USD"
                    symbol = symbol_cb.replace('-USD', 'USDT')
                    
                    if 'price' in data:
                        price = float(data['price'])
                        await self.price_handler.handle_price_update('coinbase', symbol, price)
                        # logger.info(f"✅ Coinbase price update: {symbol} = {price}")
                        
                # Обработка подписки
                # elif data.get('type') == 'subscriptions':
                #     # logger.info(f"✅ Coinbase subscription confirmed: {data}")
                    
                # Обработка ошибок
                elif data.get('type') == 'error':
                    logger.error(f"❌ Coinbase error: {data}")
                    
            except asyncio.TimeoutError:
                # Coinbase не требует ping, но проверим соединение
                try:
                    if self.is_connection_open(websocket):
                        await websocket.ping()
                except:
                    break
            except websockets.exceptions.ConnectionClosed as e:
                logger.info(f"🔌 Coinbase connection closed: {e}")
                break
            except Exception as e:
                logger.error(f"❌ Coinbase message error: {e}")
                break
        
        logger.info("🔚 Coinbase handler stopped")
    async def connect_binance(self) -> bool:
        """Binance Futures WebSocket"""
        try:
            symbols = [s.lower() for s in Config.FUTURES_SYMBOLS]
            streams = [f"{s}@markPrice@1s" for s in symbols]
            combined_streams = "/".join(streams)
            url = f"wss://fstream.binance.com/stream?streams={combined_streams}"
            
            websocket = await websockets.connect(url, ping_interval=20, ping_timeout=10)
            self.connections['binance'] = websocket
            
            asyncio.create_task(self.handle_binance_messages(websocket))
            return True
            
        except Exception as e:
            logger.error(f"Binance connection failed: {e}")
            return False

    async def handle_binance_messages(self, websocket):
        """Обработка сообщений Binance"""
        while self.is_running and self.is_connection_open(websocket):
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=30)
                data = json.loads(message)
                
                if 'data' in data:
                    symbol = data['data']['s']
                    price = float(data['data']['p'])
                    await self.price_handler.handle_price_update('binance', symbol, price,)
                    
            except asyncio.TimeoutError:
                await websocket.ping()
            except Exception as e:
                logger.error(f"Binance message error: {e}")
                break

    async def connect_bybit(self) -> bool:
        """Bybit Futures WebSocket"""
        try:
            url = "wss://stream.bybit.com/v5/public/linear"
            
            websocket = await websockets.connect(url, ping_interval=20, ping_timeout=10)
            self.connections['bybit'] = websocket
            
            subscribe_msg = {
                "op": "subscribe",
                "args": [f"tickers.{symbol}" for symbol in Config.FUTURES_SYMBOLS]
            }
            
            await websocket.send(json.dumps(subscribe_msg))
            asyncio.create_task(self.handle_bybit_messages(websocket))
            return True
            
        except Exception as e:
            logger.error(f"Bybit connection failed: {e}")
            return False

    async def handle_bybit_messages(self, websocket):
        """Обработка сообщений Bybit"""
        while self.is_running and self.is_connection_open(websocket):
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=30)
                data = json.loads(message)
                
                if data.get('op') == 'ping':
                    pong_msg = {"op": "pong"}
                    await websocket.send(json.dumps(pong_msg))
                    continue
                    
                if data.get('topic', '').startswith('tickers.'):
                    symbol = data['data']['symbol']
                    
                    if 'lastPrice' in data['data'] and data['data']['lastPrice']:
                        price = float(data['data']['lastPrice'])
                        await self.price_handler.handle_price_update('bybit', symbol, price, )
                    
            except asyncio.TimeoutError:
                ping_msg = {"op": "ping"}
                await websocket.send(json.dumps(ping_msg))
            except Exception as e:
                logger.error(f"Bybit message error: {e}")
                break

    async def connect_okx(self) -> bool:
        """OKX Futures WebSocket"""
        try:
            url = "wss://ws.okx.com:8443/ws/v5/public"
            
            websocket = await websockets.connect(url, ping_interval=20, ping_timeout=10)
            self.connections['okx'] = websocket
            
            symbols_for_okx = [f"{symbol.replace('USDT', '-USDT-SWAP')}" for symbol in Config.FUTURES_SYMBOLS]
            
            subscribe_msg = {
                "op": "subscribe",
                "args": [
                    {
                        "channel": "mark-price",
                        "instId": symbol
                    } for symbol in symbols_for_okx
                ]
            }
            
            await websocket.send(json.dumps(subscribe_msg))
            asyncio.create_task(self.handle_okx_messages(websocket))
            return True
            
        except Exception as e:
            logger.error(f"OKX connection failed: {e}")
            return False

    async def handle_okx_messages(self, websocket):
        """Обработка сообщений OKX"""
        while self.is_running and self.is_connection_open(websocket):
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=30)
                data = json.loads(message)
                
                if data.get('op') == 'ping':
                    pong_msg = {"op": "pong"}
                    await websocket.send(json.dumps(pong_msg))
                    continue
                    
                if 'arg' in data and data['arg']['channel'] == 'mark-price':
                    if 'data' in data and len(data['data']) > 0:
                        symbol = data['arg']['instId']
                        symbol = symbol.replace('-USDT-SWAP', 'USDT')
                        if 'markPx' in data['data'][0]:
                            price = float(data['data'][0]['markPx'])
                            await self.price_handler.handle_price_update('okx', symbol, price, )
                            
            except asyncio.TimeoutError:
                ping_msg = {"op": "ping"}
                await websocket.send(json.dumps(ping_msg))
            except Exception as e:
                logger.error(f"OKX message error: {e}")
                break

    async def connect_kraken(self) -> bool:
        """Kraken Futures WebSocket"""
        try:
            url = "wss://futures.kraken.com/ws/v1"
            websocket = await websockets.connect(url)
            self.connections['kraken'] = websocket
            
            # Kraken поддерживает только BTC и ETH
            symbol_map = {"BTCUSDT": "PI_XBTUSD", "ETHUSDT": "PI_ETHUSD"}
            product_ids = [symbol_map[s] for s in ['BTCUSDT', 'ETHUSDT']]
            
            subscribe_msg = {
                "event": "subscribe",
                "feed": "ticker", 
                "product_ids": product_ids
            }
            
            await websocket.send(json.dumps(subscribe_msg))
            asyncio.create_task(self.handle_kraken_messages(websocket))
            return True
            
        except Exception as e:
            logger.error(f"Kraken connection failed: {e}")
            return False

    async def handle_kraken_messages(self, websocket):
            """Обработка сообщений Kraken (FIXED: Ping Timeout)"""
            logger.info("📝 Kraken handler started")
            symbol_map = {"PI_XBTUSD": "BTCUSDT", "PI_ETHUSD": "ETHUSDT"}
            
            try:
                while self.is_running and self.is_connection_open(websocket):
                    try:
                        # Увеличиваем таймаут ожидания
                        message = await asyncio.wait_for(websocket.recv(), timeout=20)
                        data = json.loads(message)
                        
                        if 'event' in data and data['event'] == 'heartbeat':
                            continue
                            
                        if 'product_id' in data and 'markPrice' in data:
                            symbol = symbol_map.get(data['product_id'])
                            if symbol:
                                price = float(data['markPrice'])
                                await self.price_handler.handle_price_update('kraken', symbol, price)
                                
                    except asyncio.TimeoutError:
                        # Вместо отправки пинга вручную (который вызывает ошибку),
                        # просто логируем. Websockets сам управляет пингами.
                        logger.debug("⏰ Kraken: No data for 20s")
                        # Если соединение реально мертвое, следующий recv() выбросит ошибку
                        continue
                        
                    except Exception as e:
                        logger.error(f"❌ Kraken message processing error: {e}")
                        # Не выходим из цикла при ошибке парсинга, но выходим при разрыве
                        if "ConnectionClosed" in str(e) or "1011" in str(e):
                            raise e
                            
            except Exception as e:
                logger.warning(f"🔌 Kraken connection lost: {e}")
            finally:
                logger.info("🔚 Kraken handler stopped")

    async def connect_htx(self) -> bool:
        """HTX Futures WebSocket"""
        try:
            url = "wss://api.hbdm.com/linear-swap-ws"
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            websocket = await websockets.connect(url, ssl=ssl_context, ping_interval=20, ping_timeout=10)
            self.connections['htx'] = websocket

            # HTX не поддерживает MATICUSDT
            supported_symbols = [s for s in Config.FUTURES_SYMBOLS if s != 'MATICUSDT']
            
            for symbol in supported_symbols:
                htx_swap_symbol = symbol.replace("USDT", "-USDT")
                subscribe_msg = {
                    "sub": f"market.{htx_swap_symbol}.bbo",
                    "id": f"id_{int(time.time())}"
                }
                await websocket.send(json.dumps(subscribe_msg))
                await asyncio.sleep(0.1)

            asyncio.create_task(self.handle_htx_messages(websocket))
            return True

        except Exception as e:
            logger.error(f"HTX connection failed: {e}")
            return False

    async def handle_htx_messages(self, websocket):
        """Обработка сообщений HTX"""
        while self.is_running and self.is_connection_open(websocket):
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=30)
                
                if message == '{"op":"ping"}':
                    await websocket.send('{"op":"pong"}')
                    continue
                    
                if isinstance(message, bytes):
                    try:
                        decompressed = gzip.decompress(message)
                        message_str = decompressed.decode('utf-8')
                        messages = message_str.strip().split('\n')
                        for msg in messages:
                            if msg:
                                try:
                                    data = json.loads(msg)
                                    await self.process_htx_data(data)
                                except json.JSONDecodeError:
                                    continue
                    except Exception as e:
                        logger.error(f"HTX gzip error: {e}")
                else:
                    try:
                        data = json.loads(message)
                        await self.process_htx_data(data)
                    except json.JSONDecodeError:
                        continue
                        
            except asyncio.TimeoutError:
                await websocket.send('{"op":"ping"}')
            except Exception as e:
                logger.error(f"HTX message error: {e}")
                break

    async def process_htx_data(self, data):
        """Обработка данных HTX"""
        try:
            if 'ping' in data:
                pong_msg = {'pong': data['ping']}
                await self.connections['htx'].send(json.dumps(pong_msg))
                return
                
            if 'ch' in data and 'bbo' in data['ch']:
                symbol_str = data['ch'].split('.')[1].upper()
                symbol = symbol_str.replace('-', '')
                if 'tick' in data:
                    tick_data = data['tick']
                    if 'ask' in tick_data and tick_data['ask'] and 'bid' in tick_data and tick_data['bid']:
                        ask_price = float(tick_data['ask'][0])
                        bid_price = float(tick_data['bid'][0])
                        price = (ask_price + bid_price) / 2
                        await self.price_handler.handle_price_update('htx', symbol, price, )
                    elif 'last' in tick_data:
                        price = float(tick_data['last'])
                        await self.price_handler.handle_price_update('htx', symbol, price,)
                        
        except Exception as e:
            logger.error(f"HTX data processing error: {e}")

    async def connect_gateio(self) -> bool:
        """Gate.io Futures WebSocket"""
        try:
            url = "wss://fx-ws.gateio.ws/v4/ws/usdt"
            
            websocket = await websockets.connect(url, ping_interval=20, ping_timeout=10)
            self.connections['gateio'] = websocket
            
            for symbol in Config.FUTURES_SYMBOLS:
                gate_symbol = symbol.replace('USDT', '_USDT')
                subscribe_msg = {
                    "time": int(time.time()),
                    "channel": "futures.tickers",
                    "event": "subscribe", 
                    "payload": [gate_symbol]
                }
                await websocket.send(json.dumps(subscribe_msg))
                await asyncio.sleep(0.1)
            
            asyncio.create_task(self.handle_gateio_messages(websocket))
            return True
            
        except Exception as e:
            logger.error(f"Gate.io connection failed: {e}")
            return False

    async def handle_gateio_messages(self, websocket):
        """Обработка сообщений Gate.io"""
        while self.is_running and self.is_connection_open(websocket):
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=30)
                data = json.loads(message)
                
                if data.get('event') == 'update' and 'result' in data:
                    result = data['result']
                    
                    if isinstance(result, dict):
                        symbol = result.get('contract', '').replace('_', '')
                        if 'last' in result:
                            price = float(result['last'])
                            await self.price_handler.handle_price_update('gateio', symbol, price, )
                    
                    elif isinstance(result, list):
                        for ticker in result:
                            if isinstance(ticker, dict):
                                symbol = ticker.get('contract', '').replace('_', '')
                                if 'last' in ticker:
                                    price = float(ticker['last'])
                                    await self.price_handler.handle_price_update('gateio', symbol, price, )
                
            except asyncio.TimeoutError:
                ping_msg = {"event": "ping"}
                await websocket.send(json.dumps(ping_msg))
            except Exception as e:
                logger.error(f"Gate.io message error: {e}")
                break

    async def connect_bitget(self) -> bool:
        """Bitget Futures WebSocket - полностью исправленная версия"""
        try:
            # Правильный URL для фьючерсов Bitget
            url = "wss://ws.bitget.com/v2/ws/public"
            
            websocket = await websockets.connect(url, ping_interval=20, ping_timeout=10)
            self.connections['bitget'] = websocket
            
            # ПРАВИЛЬНЫЙ формат подписки для фьючерсов Bitget
            subscribe_args = []
            for symbol in Config.FUTURES_SYMBOLS:
                subscribe_args.append({
                    "instType": "USDT-FUTURES",  # ИСПРАВЛЕНО: правильный тип инструмента
                    "channel": "ticker", 
                    "instId": symbol
                })
            
            subscribe_msg = {
                "op": "subscribe",
                "args": subscribe_args
            }
            
            await websocket.send(json.dumps(subscribe_msg))
            logger.info("✅ Bitget: Correct USDT-FUTURES subscription sent")
            
            # Ждем ответа от биржи
            try:
                response = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                response_data = json.loads(response)
                logger.info(f"🔍 Bitget subscription response: {response_data}")
            except asyncio.TimeoutError:
                logger.warning("⏰ No immediate response from Bitget")
            
            asyncio.create_task(self.handle_bitget_messages(websocket))
            return True
            
        except Exception as e:
            logger.error(f"❌ Bitget connection failed: {e}")
            return False

    async def handle_bitget_messages(self, websocket):
        """Обработчик Bitget - полностью переписанный"""
        message_count = 0
        
        while self.is_running and self.is_connection_open(websocket):
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=30)
                data = json.loads(message)
                
                # Логируем первые 10 сообщений для диагностики
                if message_count < 10:
                    logger.info(f"🔍 Bitget message {message_count}: {data}")
                    message_count += 1
                
                # Обработка ping
                if data.get('op') == 'ping':
                    pong_msg = {"op": "pong"}
                    await websocket.send(json.dumps(pong_msg))
                    continue
                    
                # Обработка успешной подписки
                if data.get('event') == 'subscribe':
                    logger.info(f"✅ Bitget subscription success: {data.get('arg', {})}")
                    continue
                    
                # Обработка ошибок
                if data.get('event') == 'error':
                    logger.error(f"❌ Bitget error: {data}")
                    # Пробуем альтернативный формат подписки
                    await self.try_alternative_bitget_subscription(websocket)
                    continue
                    
                # Обработка данных тикера
                if data.get('action') == 'snapshot' and 'data' in data:
                    for ticker in data['data']:
                        symbol = ticker.get('instId')
                        if symbol and 'lastPr' in ticker:
                            price = float(ticker['lastPr'])
                            await self.price_handler.handle_price_update('bitget', symbol, price, )
                            if message_count <= 5:
                                logger.info(f"✅ Bitget price update: {symbol} = {price}")
                    
            except asyncio.TimeoutError:
                ping_msg = {"op": "ping"}
                await websocket.send(json.dumps(ping_msg))
            except Exception as e:
                logger.error(f"Bitget message error: {e}")
                break

    async def try_alternative_bitget_subscription(self, websocket):
        """Альтернативные методы подписки для Bitget"""
        alternative_methods = [
            {"instType": "USDT-FUTURES", "channel": "ticker", "instId": "BTCUSDT"},
            {"instType": "USDT-FUTURES", "channel": "ticker", "instId": "ETHUSDT"},
            {"instType": "mc", "channel": "ticker", "instId": "BTCUSDT"},
            {"instType": "mc", "channel": "ticker", "instId": "ETHUSDT"},
        ]
        
        for method in alternative_methods:
            try:
                subscribe_msg = {
                    "op": "subscribe",
                    "args": [method]
                }
                await websocket.send(json.dumps(subscribe_msg))
                logger.info(f"🔄 Bitget trying alternative: {method}")
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"Bitget alternative subscription failed: {e}")
    async def connect_mexc(self) -> bool:
        """MEXC с упрощенной стабильной версией"""
        try:
            url = "wss://contract.mexc.com/edge"
            logger.info(f"🔄 MEXC: Connecting to {url}")
            
            websocket = await websockets.connect(
                url, 
                ping_interval=25,
                ping_timeout=20,
                close_timeout=15
            )
            self.connections['mexc'] = websocket
            
            # ПРОСТАЯ ПОДПИСКА НА ВСЕ ТИКЕРЫ
            subscribe_msg = {
                "method": "sub.tickers",
                "param": {}
            }
            
            await websocket.send(json.dumps(subscribe_msg))
            logger.info("✅ MEXC: Subscribed to all tickers")
            
            asyncio.create_task(self.handle_mexc_messages_simple(websocket))
            return True
            
        except Exception as e:
            logger.error(f"❌ MEXC connection failed: {e}")
            return False

    async def handle_mexc_messages_simple(self, websocket):
        """Упрощенный обработчик MEXC"""
        logger.info("📝 MEXC simple handler started")
        
        while self.is_running and self.is_connection_open(websocket):
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=30)
                
                # Обработка ping
                if message == '{"method":"ping"}':
                    await websocket.send('{"method":"pong"}')
                    continue
                    
                data = json.loads(message)
                
                # Обработка данных тикеров
                if data.get('channel') == 'push.tickers' and 'data' in data:
                    for ticker in data['data']:
                        symbol = ticker.get('symbol', '').replace('_USDT', 'USDT')
                        if 'lastPrice' in ticker:
                            price = float(ticker['lastPrice'])
                            await self.price_handler.handle_price_update('mexc', symbol, price)
                            
            except asyncio.TimeoutError:
                # Простой ping
                try:
                    if self.is_connection_open(websocket):
                        ping_msg = {"method": "ping"}
                        await websocket.send(json.dumps(ping_msg))
                except:
                    break
            except Exception as e:
                logger.error(f"❌ MEXC message error: {e}")
                break
        
        logger.info("🔚 MEXC handler stopped")

    async def connect_bingx(self) -> bool:
        """BingX с улучшенной стабильностью и приоритетными символами"""
        try:
            url = "wss://open-api-swap.bingx.com/swap-market"
            
            logger.info(f"🔄 BingX: Connecting to {url}")
            
            websocket = await websockets.connect(
                url, 
                ping_interval=25,
                ping_timeout=20,
                close_timeout=15,
                max_queue=1024
            )
            self.connections['bingx'] = websocket
            
            # ПРИОРИТЕТНЫЕ СИМВОЛЫ ДЛЯ НАЧАЛА
            priority_symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT']
            
            successful_subscriptions = 0
            for symbol in priority_symbols:
                try:
                    bingx_symbol = symbol.replace('USDT', '-USDT')
                    subscribe_msg = {
                        "id": f"id_{int(time.time())}_{symbol}",
                        "reqType": "sub",
                        "dataType": f"{bingx_symbol}@markPrice"
                    }
                    
                    await websocket.send(json.dumps(subscribe_msg))
                    successful_subscriptions += 1
                    await asyncio.sleep(0.3)  # Увеличиваем задержку для стабильности
                    
                except Exception as e:
                    logger.error(f"❌ BingX subscription failed for {symbol}: {e}")
            
            logger.info(f"✅ BingX subscribed to {successful_subscriptions}/{len(priority_symbols)} symbols")
            
            # ЗАПУСКАЕМ ОБРАБОТЧИК
            asyncio.create_task(self.handle_bingx_messages_improved(websocket))
            return True
            
        except Exception as e:
            logger.error(f"❌ BingX connection failed: {e}")
            return False
    async def process_bingx_data_safe(self, data):
        """Безопасная обработка данных BingX с обработкой исключений"""
        try:
            await self.process_bingx_data(data)
        except Exception as e:
            logger.error(f"❌ Bingx data processing error: {e}")
            # Логируем проблемные данные для диагностики
            logger.debug(f"🔍 Problematic Bingx data: {data}")
    async def handle_bingx_messages_improved(self, websocket):
        """УЛУЧШЕННЫЙ обработчик BingX с полной защитой"""
        logger.info("📝 BingX improved handler started")
        reconnect_delay = 5
        
        while self.is_running:
            try:
                if not self.is_connection_open(websocket):
                    logger.warning("🔌 BingX connection not open, reconnecting...")
                    break
                    
                message = await asyncio.wait_for(websocket.recv(), timeout=25)
                
                if message == "Ping":
                    if self.is_connection_open(websocket):
                        await websocket.send("Pong")
                    continue
                    
                # Обработка сжатых сообщений
                if isinstance(message, bytes):
                    try:
                        decompressed = gzip.decompress(message)
                        message_str = decompressed.decode('utf-8')
                        messages = message_str.strip().split('\n')
                        for msg in messages:
                            if msg and msg != "Ping":
                                try:
                                    data = json.loads(msg)
                                    await self.process_bingx_data_safe(data)  # ИСПОЛЬЗУЕМ БЕЗОПАСНЫЙ МЕТОД
                                except json.JSONDecodeError as e:
                                    logger.debug(f"Bingx JSON decode error: {e}")
                                    continue
                    except gzip.BadGzipFile:
                        # Если это не gzip, пробуем обработать как обычный текст
                        try:
                            message_str = message.decode('utf-8')
                            if message_str and message_str != "Ping":
                                data = json.loads(message_str)
                                await self.process_bingx_data_safe(data)
                        except (UnicodeDecodeError, json.JSONDecodeError) as e:
                            logger.debug(f"Bingx text decode error: {e}")
                    except Exception as e:
                        logger.error(f"❌ Bingx message processing error: {e}")
                else:
                    # Обработка текстовых сообщений
                    if message != "Ping":
                        try:
                            data = json.loads(message)
                            await self.process_bingx_data_safe(data)  # ИСПОЛЬЗУЕМ БЕЗОПАСНЫЙ МЕТОД
                        except json.JSONDecodeError as e:
                            logger.debug(f"Bingx text JSON error: {e} - Message: {message}")
                            
            except asyncio.TimeoutError:
                # БЕЗОПАСНЫЙ ping
                try:
                    if self.is_connection_open(websocket):
                        await websocket.send("Ping")
                except Exception as e:
                    logger.debug(f"BingX timeout ping failed: {e}")
                    break
                    
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"🔌 BingX connection closed: {e}")
                break
            except Exception as e:
                logger.error(f"❌ BingX message handler error: {e}")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)
                break
        
        logger.info("🔚 BingX handler stopped")
        # Помечаем биржу для переподключения
        self.failed_exchanges.add('bingx')
        self.connected_exchanges.discard('bingx')

    async def process_bingx_data(self, data):
        """Обработка данных Bingx с дополнительной защитой"""
        try:
            # ПРОВЕРКА НАЛИЧИЯ ВСЕХ НЕОБХОДИМЫХ ПОЛЕЙ
            if not data or not isinstance(data, dict):
                return
                
            # Обработка ping/pong
            if data.get('ping'):
                pong_msg = {'pong': data['ping']}
                if 'bingx' in self.connections and self.is_connection_open(self.connections['bingx']):
                    await self.connections['bingx'].send(json.dumps(pong_msg))
                return
            
            # Основная логика обработки цен
            if 'code' in data and data['code'] == 0 and 'dataType' in data and 'data' in data:
                data_type = data['dataType']
                if '@markPrice' in data_type:
                    symbol_with_dash = data_type.split('@')[0]
                    symbol = symbol_with_dash.replace('-USDT', 'USDT')
                    price_data = data['data']
                    
                    # ДОПОЛНИТЕЛЬНЫЕ ПРОВЕРКИ ЦЕНЫ
                    if 'p' in price_data and price_data['p']:
                        try:
                            price = float(price_data['p'])
                            if price > 0:  # Проверяем что цена валидная
                                await self.price_handler.handle_price_update('bingx', symbol, price)
                            else:
                                logger.warning(f"⚠️ Bingx invalid price: {price} for {symbol}")
                        except (ValueError, TypeError) as e:
                            logger.error(f"❌ Bingx price conversion error: {e} for data: {price_data}")
                            
        except Exception as e:
            logger.error(f"❌ Bingx data processing error: {e}")
            # Логируем стектрейс для диагностики
            import traceback
            logger.debug(f"🔍 Bingx error traceback: {traceback.format_exc()}")

    async def connect_kucoin(self) -> bool:
        """KuCoin с правильными символами для Futures"""
        try:
            logger.info("🚀 STARTING KUCONN WITH FUTURES SYMBOLS")
            
            # Получаем токен
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    'https://api-futures.kucoin.com/api/v1/bullet-public',
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    data = await resp.json()
                    
                    if data['code'] != '200000':
                        logger.error(f"❌ KuCoin token error: {data}")
                        return False
                    
                    endpoint = data['data']['instanceServers'][0]['endpoint']
                    token = data['data']['token']
                    url = f"{endpoint}?token={token}&connectId={int(time.time())}"
            
            logger.info(f"🔗 KuCoin connecting to: {url}")
            
            websocket = await websockets.connect(url, ping_interval=20, ping_timeout=10)
            self.connections['kucoin'] = websocket
            
            # Ждем welcome сообщение
            welcome = await asyncio.wait_for(websocket.recv(), timeout=5.0)
            logger.info(f"🔍 KuCoin welcome: {welcome}")
            
            # ИСПРАВЛЕНИЕ: Используем правильные символы для KuCoin Futures
            # KuCoin Futures использует XBT вместо BTC и другие форматы
            kucoin_symbols = {
                'BTCUSDT': 'XBTUSDT',  # KuCoin использует XBT для Bitcoin в фьючерсах
                'ETHUSDT': 'ETHUSDT',
                'BNBUSDT': 'BNBUSDT', 
                'SOLUSDT': 'SOLUSDT',
                'ADAUSDT': 'ADAUSDT',
            }
            
            subscriptions = []
            for standard_symbol, kucoin_symbol in kucoin_symbols.items():
                subscriptions.extend([
                    {
                        "id": len(subscriptions) + 1, 
                        "type": "subscribe", 
                        "topic": f"/contractMarket/ticker:{kucoin_symbol}",
                        "privateChannel": False, 
                        "response": True
                    },
                    {
                        "id": len(subscriptions) + 2,
                        "type": "subscribe",
                        "topic": f"/contractMarket/snapshot:{kucoin_symbol}",
                        "privateChannel": False,
                        "response": True
                    }
                ])
            
            for sub in subscriptions:
                await websocket.send(json.dumps(sub))
                logger.info(f"📨 KuCoin subscribing: {sub['topic']}")
                
                # Ждем подтверждение для каждой подписки
                try:
                    ack = await asyncio.wait_for(websocket.recv(), timeout=3.0)
                    ack_data = json.loads(ack)
                    logger.info(f"✅ KuCoin subscription ack: {ack_data}")
                    
                except asyncio.TimeoutError:
                    logger.warning(f"⏰ KuCoin no ack for {sub['topic']}")
                
                await asyncio.sleep(0.3)
            
            # Создаем mapping для обратного преобразования символов
            self.kucoin_symbol_mapping = {v: k for k, v in kucoin_symbols.items()}
            
            # Запускаем обработчик
            asyncio.create_task(self.kucoin_futures_handler(websocket))
            
            logger.info("✅ KuCoin futures connection established")
            return True
            
        except Exception as e:
            logger.error(f"❌ KuCoin connection failed: {e}")
            return False

    async def kucoin_futures_handler(self, websocket):
        """Обработчик для KuCoin Futures с преобразованием символов"""
        logger.info("📝 STARTING KUCONN FUTURES HANDLER")
        message_count = 0
        
        while self.is_running and self.is_connection_open(websocket):
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=20)
                message_count += 1
                
                # Логируем КАЖДОЕ сообщение
                logger.info(f"🔍 KUCONN FUTURES #{message_count}: {message}")
                
                data = json.loads(message)
                
                # Обработка ping/pong
                if data.get('type') == 'ping':
                    pong_msg = {"id": data.get('id'), "type": "pong"}
                    await websocket.send(json.dumps(pong_msg))
                    continue
                    
                if data.get('type') == 'pong':
                    continue
                
                # Обработка данных
                if data.get('type') == 'message' and 'data' in data:
                    await self.process_kucoin_futures_data(data)
                    
            except asyncio.TimeoutError:
                # Пинг при таймауте
                ping_msg = {"id": int(time.time() * 1000), "type": "ping"}
                await websocket.send(json.dumps(ping_msg))
            except Exception as e:
                logger.error(f"❌ KuCoin futures handler error: {e}")
                break

    async def process_kucoin_futures_data(self, data):
        """Обработка данных KuCoin Futures с преобразованием символов"""
        try:
            topic = data.get('topic', '')
            message_data = data.get('data', {})
            
            # Извлекаем символ KuCoin из topic
            kucoin_symbol = None
            if ':' in topic:
                kucoin_symbol = topic.split(':')[-1]
            
            if not kucoin_symbol:
                return
            
            # Преобразуем символ KuCoin в стандартный
            standard_symbol = self.kucoin_symbol_mapping.get(kucoin_symbol)
            if not standard_symbol:
                logger.warning(f"⚠️ Unknown KuCoin symbol: {kucoin_symbol}")
                return
            
            logger.info(f"🔍 KuCoin processing: {kucoin_symbol} -> {standard_symbol}")
            
            # Ищем цену в данных
            price = None
            
            # Для ticker данных
            if 'ticker' in topic:
                price_fields = ['price', 'lastTradedPrice', 'lastPrice', 'markPrice']
                for field in price_fields:
                    if field in message_data and message_data[field]:
                        try:
                            price = float(message_data[field])
                            break
                        except (ValueError, TypeError):
                            continue
            
            # Для snapshot данных
            elif 'snapshot' in topic:
                if 'lastPrice' in message_data and message_data['lastPrice']:
                    try:
                        price = float(message_data['lastPrice'])
                    except (ValueError, TypeError):
                        pass
            
            if price and price > 0:
                await self.price_handler.handle_price_update('kucoin', standard_symbol, price, )
                logger.info(f"🎯 KUCONN FUTURES PRICE: {standard_symbol} = {price}")
            else:
                logger.warning(f"⚠️ KuCoin no price in data: {message_data}")
                
        except Exception as e:
            logger.error(f"❌ KuCoin futures data processing error: {e}")

    async def kucoin_enhanced_handler(self, websocket):
        """УСИЛЕННЫЙ обработчик KuCoin с максимальным логированием"""
        logger.info("📝 STARTING KUCONN ENHANCED HANDLER")
        message_count = 0
        last_ping = time.time()
        
        while self.is_running and self.is_connection_open(websocket):
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=20)
                message_count += 1
                
                # Логируем АБСОЛЮТНО ВСЕ сообщения
                logger.info(f"🔍 KUCONN ENHANCED #{message_count}: {message}")
                
                data = json.loads(message)
                
                # Обработка ping/pong
                if data.get('type') == 'ping':
                    pong_msg = {"id": data.get('id'), "type": "pong"}
                    await websocket.send(json.dumps(pong_msg))
                    logger.info("✅ KuCoin answered ping")
                    continue
                    
                if data.get('type') == 'pong':
                    logger.info("✅ KuCoin received pong")
                    continue
                
                # Обработка ack сообщений
                if data.get('type') == 'ack':
                    logger.info(f"✅ KuCoin ack: {data}")
                    continue
                    
                # Обработка ошибок
                if data.get('type') == 'error':
                    logger.error(f"❌ KuCoin error: {data}")
                    continue
                
                # ОСНОВНАЯ ЛОГИКА: Обработка данных
                if data.get('type') == 'message':
                    topic = data.get('topic', '')
                    logger.info(f"🎯 KuCoin MESSAGE topic: {topic}")
                    
                    if 'data' in data:
                        await self.process_kucoin_message_data(data)
                
                # Регулярный ping каждые 30 секунд
                if time.time() - last_ping > 30:
                    ping_msg = {"id": int(time.time() * 1000), "type": "ping"}
                    await websocket.send(json.dumps(ping_msg))
                    last_ping = time.time()
                    logger.info("📨 KuCoin sent periodic ping")
                    
            except asyncio.TimeoutError:
                # Пинг при таймауте
                ping_msg = {"id": int(time.time() * 1000), "type": "ping"}
                await websocket.send(json.dumps(ping_msg))
                logger.info("📨 KuCoin sent timeout ping")
            except Exception as e:
                logger.error(f"❌ KuCoin enhanced handler error: {e}")
                break

    async def process_kucoin_message_data(self, data):
        """Обработка данных сообщения KuCoin"""
        try:
            topic = data.get('topic', '')
            message_data = data.get('data', {})
            
            logger.info(f"🔍 KuCoin processing topic: {topic}")
            logger.info(f"🔍 KuCoin data: {message_data}")
            
            # Определяем символ из topic
            symbol = None
            if ':' in topic:
                symbol = topic.split(':')[-1]
            
            if not symbol:
                logger.warning(f"⚠️ KuCoin cannot determine symbol from topic: {topic}")
                return
            
            # Обработка разных типов topic
            if 'ticker' in topic:
                await self.process_kucoin_ticker(symbol, message_data)
            elif 'snapshot' in topic:
                await self.process_kucoin_snapshot(symbol, message_data)
            elif 'trade' in topic:
                await self.process_kucoin_trade(symbol, message_data)
            else:
                logger.info(f"🔍 KuCoin unknown topic type: {topic}")
                
        except Exception as e:
            logger.error(f"❌ KuCoin message data processing error: {e}")

    async def process_kucoin_ticker(self, symbol, ticker_data):
        """Обработка тикера KuCoin"""
        try:
            price = None
            
            # Пробуем разные поля с ценой
            price_fields = ['price', 'lastTradedPrice', 'lastPrice', 'markPrice']
            
            for field in price_fields:
                if field in ticker_data and ticker_data[field]:
                    try:
                        price_val = ticker_data[field]
                        if isinstance(price_val, (int, float)) and price_val > 0:
                            price = float(price_val)
                            break
                        elif isinstance(price_val, str) and price_val.strip() and price_val != '0':
                            price = float(price_val)
                            break
                    except (ValueError, TypeError):
                        continue
            
            if price and price > 0:
                await self.price_handler.handle_price_update('kucoin', symbol, price, )
                logger.info(f"🎯 KUCONN TICKER PRICE: {symbol} = {price}")
            else:
                logger.warning(f"⚠️ KuCoin ticker no price found: {ticker_data}")
                
        except Exception as e:
            logger.error(f"❌ KuCoin ticker processing error: {e}")

    async def process_kucoin_snapshot(self, symbol, snapshot_data):
        """Обработка снапшота KuCoin"""
        try:
            price = None
            
            # В снапшоте ищем lastPrice
            if 'lastPrice' in snapshot_data and snapshot_data['lastPrice']:
                try:
                    price = float(snapshot_data['lastPrice'])
                except (ValueError, TypeError):
                    pass
            
            if price and price > 0:
                await self.price_handler.handle_price_update('kucoin', symbol, price, )
                logger.info(f"🎯 KUCONN SNAPSHOT PRICE: {symbol} = {price}")
            else:
                logger.warning(f"⚠️ KuCoin snapshot no price found: {snapshot_data}")
                
        except Exception as e:
            logger.error(f"❌ KuCoin snapshot processing error: {e}")
    async def connect_phemex(self) -> bool:
        """Исправленное подключение Phemex - используем только рабочие методы"""
        try:
            logger.info("🚀 STARTING PHEMEX FIXED CONNECTION")
            
            url = "wss://ws.phemex.com"
            logger.info(f"🔗 Phemex connecting to: {url}")
            
            websocket = await websockets.connect(url, ping_interval=30, ping_timeout=20)
            self.connections['phemex'] = websocket
            
            # Создаем задачу для логирования
            asyncio.create_task(self.phemex_raw_logger_fixed(websocket))
            
            # ИСПРАВЛЕНИЕ: Используем ТОЛЬКО рабочие методы из логов
            # tick.subscribe для ETHUSDT работает - расширим на другие символы
            working_symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT', 'XRPUSDT']
            
            for i, symbol in enumerate(working_symbols):
                try:
                    method = {
                        "id": i + 1, 
                        "method": "tick.subscribe", 
                        "params": [symbol]
                    }
                    await websocket.send(json.dumps(method))
                    logger.info(f"📨 Phemex subscribing to ticker: {symbol}")
                    await asyncio.sleep(0.2)  # Задержка между подписками
                except Exception as e:
                    logger.error(f"❌ Phemex subscription for {symbol} failed: {e}")
            
            logger.info("✅ Phemex fixed connection established")
            return True
            
        except Exception as e:
            logger.error(f"❌ Phemex connection failed: {e}")
            return False

    async def phemex_raw_logger_fixed(self, websocket):
        """Исправленный обработчик Phemex - полная защита от None"""
        logger.info("📝 STARTING PHEMEX FIXED MESSAGE LOGGER")
        message_count = 0
        
        while self.is_running and self.is_connection_open(websocket):
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=30)
                message_count += 1
                
                # ЗАЩИТА: Проверяем что message не None и не пустой
                if not message:
                    logger.warning("🔍 PHEMEX EMPTY MESSAGE")
                    continue
                    
                try:
                    data = json.loads(message)
                except json.JSONDecodeError as e:
                    logger.warning(f"🔍 PHEMEX NON-JSON: {message}")
                    continue
                
                # Логируем ВСЕ сообщения первые 50
                if message_count <= 50:
                    logger.info(f"🔍 PHEMEX FIXED #{message_count}: {data}")
                
                # Обработка ping - ЗАЩИТА ОТ None
                if data and isinstance(data, dict) and data.get('method') == 'server.ping':
                    pong_msg = {'method': 'server.pong', 'params': []}
                    await websocket.send(json.dumps(pong_msg))
                    continue
                
                # ОСНОВНОЙ ФОКУС: обработка tick.update - ЗАЩИТА ОТ None
                if data and isinstance(data, dict) and data.get('method') == 'tick.update' and 'params' in data:
                    await self.process_phemex_tick_update(data)
                
                # Обработка успешных подписок - ПОЛНАЯ ЗАЩИТА ОТ None
                if (data and isinstance(data, dict) and 
                    data.get('result') and 
                    isinstance(data.get('result'), dict) and 
                    data.get('result').get('status') == 'success'):
                    logger.info(f"✅ Phemex subscription success: {data}")
                    
            except asyncio.TimeoutError:
                ping_msg = {'method': 'server.ping', 'params': []}
                await websocket.send(json.dumps(ping_msg))
            except Exception as e:
                logger.error(f"❌ Phemex logger error: {e}")
                # Логируем стектрейс для диагностики
                import traceback
                logger.error(f"🔍 Phemex error traceback: {traceback.format_exc()}")
                # НЕ выходим из цикла, продолжаем работу
                await asyncio.sleep(5)

    async def process_phemex_tick_update(self, data):
        """Специализированная обработка tick.update для Phemex"""
        try:
            params = data['params']
            if len(params) >= 2:
                symbol = params[0]
                tick_data = params[1]
                
                logger.info(f"🎯 PHEMEX TICK UPDATE: {symbol} - {tick_data}")
                
                # Извлекаем цену из tick данных
                price = None
                if 'last' in tick_data and tick_data['last']:
                    price = float(tick_data['last'])
                elif 'markPrice' in tick_data and tick_data['markPrice']:
                    price = float(tick_data['markPrice'])
                elif 'indexPrice' in tick_data and tick_data['indexPrice']:
                    price = float(tick_data['indexPrice'])
                elif 'fairPrice' in tick_data and tick_data['fairPrice']:
                    price = float(tick_data['fairPrice'])
                
                if price and price > 0:
                    await self.price_handler.handle_price_update('phemex', symbol, price, )
                    logger.info(f"✅ PHEMEX PRICE UPDATED: {symbol} = {price}")
                else:
                    logger.warning(f"⚠️ Phemex no price in tick data: {tick_data}")
                    
        except Exception as e:
            logger.error(f"Phemex tick processing error: {e}")
    async def handle_phemex_messages_improved(self, websocket):
        """УЛУЧШЕННЫЙ обработчик Phemex с детальным логированием"""
        message_count = 0
        
        while self.is_running and self.is_connection_open(websocket):
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=30)
                data = json.loads(message)
                
                # Логируем ВСЕ сообщения для диагностики
                if message_count < 20:
                    logger.info(f"🔍 Phemex RAW message {message_count}: {data}")
                    message_count += 1
                
                # Обработка ping от сервера
                if data.get('method') == 'server.ping':
                    pong_msg = {'method': 'server.pong', 'params': []}
                    await websocket.send(json.dumps(pong_msg))
                    logger.debug("✅ Phemex answered ping")
                    continue
                    
                # Обработка ответа на подписку
                if data.get('result') == 'ok':
                    logger.info(f"✅ Phemex subscription success: {data}")
                    continue
                    
                # Обработка ошибок
                if data.get('error'):
                    logger.error(f"❌ Phemex error: {data}")
                    continue
                
                # Обработка рыночных данных
                await self.process_phemex_market_data(data)
                    
            except asyncio.TimeoutError:
                # Отправляем ping для поддержания соединения
                ping_msg = {'method': 'server.ping', 'params': []}
                await websocket.send(json.dumps(ping_msg))
                logger.debug("🔍 Phemex sent ping")
            except Exception as e:
                if "1000 (OK)" in str(e):
                    logger.debug("Phemex connection closed normally")
                    break
                else:
                    logger.error(f"Phemex message error: {e}")
                    break

    async def process_phemex_market_data(self, data):
        """Обработка рыночных данных Phemex"""
        try:
            # Обработка тиковых данных
            if data.get('method') == 'tick.update' and 'params' in data:
                params = data['params']
                if len(params) >= 2:
                    symbol = params[0]
                    tick_data = params[1]
                    
                    # Получаем последнюю цену
                    if 'last' in tick_data:
                        price = float(tick_data['last'])
                        await self.price_handler.handle_price_update('phemex', symbol, price, )
                        logger.info(f"✅ Phemex TICK price: {symbol} = {price}")
                    elif 'markPrice' in tick_data:
                        price = float(tick_data['markPrice'])
                        await self.price_handler.handle_price_update('phemex', symbol, price, )
                        logger.info(f"✅ Phemex MARK price: {symbol} = {price}")
            
            # Обработка стакана
            elif data.get('method') == 'orderbook' and 'params' in data:
                params = data['params']
                if len(params) >= 2:
                    symbol = params[0]
                    orderbook_data = params[1]
                    
                    # Берем среднюю цену из лучших bid/ask
                    if 'bids' in orderbook_data and 'asks' in orderbook_data:
                        bids = orderbook_data['bids']
                        asks = orderbook_data['asks']
                        
                        if bids and asks:
                            best_bid = float(bids[0][0])
                            best_ask = float(asks[0][0])
                            mid_price = (best_bid + best_ask) / 2
                            
                            await self.price_handler.handle_price_update('phemex', symbol, mid_price, )
                            logger.info(f"✅ Phemex ORDERBOOK price: {symbol} = {mid_price}")
            
            # Обработка торгов
            elif data.get('method') == 'trade' and 'params' in data:
                params = data['params']
                if len(params) >= 2:
                    symbol = params[0]
                    trades = params[1]
                    if trades and len(trades) > 0:
                        # Берем последнюю сделку
                        last_trade = trades[0]
                        if 'price' in last_trade:
                            price = float(last_trade['price'])
                            await self.price_handler.handle_price_update('phemex', symbol, price, )
                            logger.info(f"✅ Phemex TRADE price: {symbol} = {price}")
                            
        except Exception as e:
            logger.error(f"Phemex market data processing error: {e}")
            logger.error(f"Problematic data: {data}")

    async def stop(self):
        """Остановка всех соединений"""
        self.is_running = False
        
        for exchange, websocket in self.connections.items():
            if websocket and not websocket.closed:
                await websocket.close()
        
        # Отменяем все задачи подключения
        for task in self.connection_tasks.values():
            task.cancel()
    async def reconnect_all_exchanges(self):
        """Принудительное переподключение ко всем биржам"""
        logger.info("🔄 Forcing reconnection to all exchanges...")
        
        # Закрываем все соединения
        for exchange, websocket in self.connections.items():
            if websocket and not websocket.closed:
                try:
                    await websocket.close()
                except:
                    pass
        
        self.connections.clear()
        self.connected_exchanges.clear()
        
        # Перезапускаем все задачи подключения
        for exchange in Config.EXCHANGES:
            if exchange in self.connection_tasks:
                self.connection_tasks[exchange].cancel()
            
            task = asyncio.create_task(self.manage_exchange_connection(exchange))
            self.connection_tasks[exchange] = task
        
        logger.info("✅ All exchange reconnection tasks started")
    async def monitor_connection_health(self):
        """Мониторинг здоровья всех соединений"""
        while self.is_running:
            try:
                await asyncio.sleep(60)  # Проверка каждую минуту
                
                health_report = {}
                for exchange in Config.EXCHANGES:
                    # Проверяем активность биржи
                    active_symbols = 0
                    for symbol in Config.FUTURES_SYMBOLS:
                        if self.price_handler.is_price_fresh(exchange, symbol):
                            active_symbols += 1
                    
                    health_report[exchange] = {
                        'active_symbols': active_symbols,
                        'is_connected': exchange in self.connected_exchanges,
                        'is_failed': exchange in self.failed_exchanges,
                        'updates_count': self.price_handler.exchange_updates.get(exchange, 0)
                    }
                
                # Логируем отчет о здоровье
                logger.info("🏥 CONNECTION HEALTH REPORT:")
                for exchange, health in health_report.items():
                    status = "✅" if health['active_symbols'] > 0 else "❌"
                    logger.info(f"  {status} {exchange}: {health['active_symbols']} symbols, "
                            f"{health['updates_count']} updates")
                            
            except Exception as e:
                logger.error(f"Health monitor error: {e}")
    async def check_and_reconnect_failed_exchanges(self):
        """Проверка и переподключение неудачных и проблемных бирж"""
        try:
            current_time = time.time()
            current_stats = self.price_handler.get_exchange_stats()
            
            # Переподключаем не только failed, но и connected биржи без активных символов
            exchanges_to_reconnect = []
            
            for exchange in Config.EXCHANGES:
                symbols_count = current_stats.get(exchange, 0)
                is_connected = exchange in self.connected_exchanges
                is_failed = exchange in self.failed_exchanges
                
                # Критерии для переподключения:
                # 1. Биржа в failed
                # 2. Биржа подключена, но нет активных символов
                # 3. Недавно не пытались переподключаться
                should_reconnect = (
                    is_failed or 
                    (is_connected and symbols_count == 0)
                ) and (current_time - self.last_reconnect_attempt.get(exchange, 0) > 30)
                
                if should_reconnect:
                    exchanges_to_reconnect.append(exchange)
            
            for exchange in exchanges_to_reconnect:
                logger.info(f"🔄 Attempting to reconnect exchange: {exchange}")
                self.last_reconnect_attempt[exchange] = current_time
                
                # Запускаем переподключение
                if exchange in self.connection_tasks:
                    self.connection_tasks[exchange].cancel()
                    
                task = asyncio.create_task(self.manage_exchange_connection(exchange))
                self.connection_tasks[exchange] = task
                await asyncio.sleep(1)  # Задержка между переподключениями
                
        except Exception as e:
            logger.error(f"Error in reconnect check: {e}")

    async def reconnect_single_exchange(self, exchange: str):
        """Переподключение одной биржи"""
        try:
            # Отменяем существующую задачу если есть
            if exchange in self.connection_tasks:
                self.connection_tasks[exchange].cancel()
            
            # Закрываем существующее соединение
            if exchange in self.connections and self.connections[exchange]:
                try:
                    await self.connections[exchange].close()
                except:
                    pass
                self.connections[exchange] = None
            
            # Убираем из подключенных и неудачных
            self.connected_exchanges.discard(exchange)
            self.failed_exchanges.discard(exchange)
            
            # Запускаем новую задачу подключения
            task = asyncio.create_task(self.manage_exchange_connection(exchange))
            self.connection_tasks[exchange] = task
            
            logger.info(f"✅ Задача переподключения запущена для {exchange}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка переподключения {exchange}: {e}")
    async def test_exchange_connection(self, exchange: str):
        """Тестирование подключения к бирже с детальной диагностикой"""
        logger.info(f"🔧 ЗАПУСК ДИАГНОСТИКИ {exchange.upper()}")
        
        if exchange == 'kucoin':
            await self.test_kucoin_connection()
        elif exchange == 'phemex':
            await self.test_phemex_connection()

    async def test_kucoin_connection(self):
        """Детальное тестирование KuCoin"""
        try:
            # Тест 1: Получение токена
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    'https://api-futures.kucoin.com/api/v1/bullet-public',
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    data = await resp.json()
                    logger.info(f"🔍 KuCoin token response: {data}")
                    
                    if data['code'] != '200000':
                        logger.error(f"❌ KuCoin token failed: {data}")
                        return False
                    
                    endpoint = data['data']['instanceServers'][0]['endpoint']
                    token = data['data']['token']
                    url = f"{endpoint}?token={token}&connectId={int(time.time())}"
                    
            logger.info(f"🔗 KuCoin WebSocket URL: {url}")
            
            # Тест 2: Подключение WebSocket
            websocket = await websockets.connect(url)
            
            # Тест 3: Подписка на один символ с детальным логированием
            test_symbol = 'BTCUSDT'
            
            # Вариант 1: Стандартная подписка
            subscribe_msg = {
                "id": int(time.time() * 1000),
                "type": "subscribe",
                "topic": f"/contractMarket/ticker:{test_symbol}",
                "privateChannel": False,
                "response": True
            }
            
            logger.info(f"📨 KuCoin sending: {subscribe_msg}")
            await websocket.send(json.dumps(subscribe_msg))
            
            # Ждем все ответы в течение 10 секунд
            start_time = time.time()
            while time.time() - start_time < 10:
                try:
                    response = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                    response_data = json.loads(response)
                    logger.info(f"🔍 KuCoin response: {response_data}")
                    
                    # Если получили ack, считаем успешным
                    if response_data.get('type') == 'ack':
                        logger.info("✅ KuCoin subscription ACK received")
                        break
                        
                except asyncio.TimeoutError:
                    logger.warning("⏰ KuCoin timeout waiting for response")
                    break
            
            await websocket.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ KuCoin diagnostic failed: {e}")
            return False
# ==================== ВЫСОКОЧАСТОТНЫЙ АРБИТРАЖНЫЙ КАЛЬКУЛЯТОР ====================

class HighFrequencyArbitrageCalculator:
    def __init__(self, price_handler: HighFrequencyPriceHandler):
        self.price_handler = price_handler
        self.opportunities_cache = []
        self.last_calculation = 0
        self.calculation_interval = 0.05  # 50ms между расчетами для 20Hz частоты
        
    def find_opportunities(self) -> List[Dict]:
        """Сверхбыстрый поиск арбитражных возможностей с кэшированием"""
        current_time = time.time()
        
        # Используем кэш для избежания лишних расчетов
        if current_time - self.last_calculation < self.calculation_interval:
            return self.opportunities_cache
        
        opportunities = []
        
        for symbol in Config.FUTURES_SYMBOLS:
            symbol_opps = self.find_symbol_opportunities_fast(symbol)
            opportunities.extend(symbol_opps)
        
        # Быстрая сортировка и кэширование
        opportunities.sort(key=lambda x: x['spread_percent'], reverse=True)
        self.opportunities_cache = opportunities[:20]
        self.last_calculation = current_time
        
        return opportunities

    def find_symbol_opportunities_fast(self, symbol: str) -> List[Dict]:
        """Оптимизированный поиск возможностей для символа"""
        opportunities = []
        prices = self.price_handler.get_current_prices(symbol)
        
        if len(prices) < 2:
            return opportunities

        exchanges = list(prices.keys())
        price_values = list(prices.values())
        
        # Быстрый поиск минимальной и максимальной цены
        min_price = min(price_values)
        max_price = max(price_values)
        
        if min_price <= 0 or max_price <= 0:
            return opportunities
        
        spread_percent = ((max_price - min_price) / min_price) * 100
        
        if spread_percent >= Config.OPEN_SPREAD_THRESHOLD:
            # Находим биржи с минимальной и максимальной ценой
            min_exchange = None
            max_exchange = None
            
            for exchange, price in prices.items():
                if price == min_price:
                    min_exchange = exchange
                if price == max_price:
                    max_exchange = exchange
                if min_exchange and max_exchange:
                    break
            
            if min_exchange and max_exchange and min_exchange != max_exchange:
                opportunity = {
                    'symbol': symbol,
                    'buy_exchange': min_exchange,
                    'sell_exchange': max_exchange,
                    'buy_price': min_price,
                    'sell_price': max_price,
                    'spread_usdt': max_price - min_price,
                    'spread_percent': spread_percent,
                    'timestamp': datetime.now()
                }
                opportunities.append(opportunity)
        
        return opportunities

# ==================== ВЫСОКОЧАСТОТНЫЙ ТРЕЙДИНГ СИМУЛЯТОР ====================

class HighFrequencyTradingSimulator:
    def __init__(self):
        self.open_trades = []
        self.closed_trades = []
        self.trade_id_counter = 0
        self.csv_file = f'futures_arbitrage_results/trades_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        self._csv_buffer = []
        self._last_csv_flush = time.time()
        self.setup_csv()

        self.symbol_trade_count = defaultdict(int)    # Счетчик сделок по символам
        self.last_update_time = time.time()
        self.active_trade_pairs = set()  # Множество активных пар (symbol, buy_exchange, sell_exchange)
        self.exchange_trade_count = defaultdict(int)  # Счетчик сделок по биржам
        self.symbol_trade_count = defaultdict(int) 
        self.active_trade_keys = set()

    def is_trade_active(self, symbol: str, buy_exchange: str, sell_exchange: str) -> bool:
        """Проверяет, есть ли уже активная сделка для данной комбинации символ+биржи"""
        trade_key = (symbol, buy_exchange, sell_exchange)
        return trade_key in self.active_trade_keys
    
    def add_active_trade(self, symbol: str, buy_exchange: str, sell_exchange: str):
        """Добавляет сделку в множество активных"""
        trade_key = (symbol, buy_exchange, sell_exchange)
        self.active_trade_keys.add(trade_key)
    
    def remove_active_trade(self, symbol: str, buy_exchange: str, sell_exchange: str):
        """Удаляет сделку из множества активных"""
        trade_key = (symbol, buy_exchange, sell_exchange)
        self.active_trade_keys.discard(trade_key)
    def setup_csv(self):
        """Быстрая настройка CSV с буферизацией записи"""
        os.makedirs('futures_arbitrage_results', exist_ok=True)
        with open(self.csv_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow([
                'ID_сделки', 'Символ', 'Биржа_покупки', 'Биржа_продажи',
                'Цена_покупки_открытие', 'Цена_продажи_открытие',
                'Цена_покупки_закрытие', 'Цена_продажи_закрытие',
                'Объем', 
                'Валовая_прибыль', 'Комиссии_общие', 'Чистая_прибыль', 'Чистая_прибыль_процент',
                'Время_открытия', 'Время_закрытия', 'Длительность_секунды',
                'Максимальный_спред_процент',
                'Время_выше_порога_секунды',
                'Статус'
            ])

    def format_number(self, number):
        """Быстрое форматирование числа с запятой"""
        if number is None:
            return "0"
        if isinstance(number, (int, float)):
            return f"{number:.8f}".replace('.', ',')
        return str(number)

    def update_trade_metrics(self, trade_id: str, current_prices: Dict[str, float]):
        """Оптимизированное обновление метрик с отслеживанием времени выше порога"""
        current_time = time.time()
        
        for trade in self.open_trades:
            if trade['trade_id'] == trade_id:
                try:
                    # Получаем время последнего обновления для этой сделки
                    last_update = trade.get('last_metrics_update', current_time)
                    delta_time = current_time - last_update
                    trade['last_metrics_update'] = current_time
                    
                    buy_price = current_prices.get(trade['buy_exchange'])
                    sell_price = current_prices.get(trade['sell_exchange'])
                    
                    if buy_price and sell_price:
                        current_spread = ((sell_price - buy_price) / buy_price) * 100
                        trade['current_spread'] = current_spread
                        
                        # Обновляем максимальный спред
                        if current_spread > trade.get('max_spread', 0):
                            trade['max_spread'] = current_spread
                        
                        # ВАЖНО: Отслеживаем время выше порога открытия
                        if current_spread >= Config.OPEN_SPREAD_THRESHOLD:
                            # Увеличиваем счетчик времени выше порога
                            trade['time_above_threshold'] = trade.get('time_above_threshold', 0) + delta_time
                        
                    # Обновляем время последнего обновления
                    trade['last_metrics_update'] = current_time
                    
                except Exception as e:
                    logger.debug(f"Ошибка обновления метрик: {e}")
                break

    def open_trade(self, opportunity: Dict, current_prices: Dict) -> str:
        """Сверхбыстрое открытие сделки с проверкой ограничений и блокировкой комбинации символ+биржи"""
        try:
            symbol = opportunity['symbol']
            buy_exchange = opportunity['buy_exchange']
            sell_exchange = opportunity['sell_exchange']
            
            # СТРОГАЯ ПРОВЕРКА: Убеждаемся, что нет активной сделки для этой комбинации символ+биржи
            if self.is_trade_active(symbol, buy_exchange, sell_exchange):
                logger.debug(f"🚫 Активная сделка уже существует: {symbol} {buy_exchange}→{sell_exchange}")
                return None
            
            # Проверяем все остальные ограничения
            if not self.can_open_trade(buy_exchange, sell_exchange, symbol):
                return None

            current_buy_price = current_prices.get(buy_exchange)
            current_sell_price = current_prices.get(sell_exchange)
            
            if not current_buy_price or not current_sell_price:
                return None
                
            current_spread = ((current_sell_price - current_buy_price) / current_buy_price) * 100
            
            if current_spread < Config.OPEN_SPREAD_THRESHOLD:
                return None

            trade_id = f"trade_{self.trade_id_counter}_{int(time.time()*1000)}"
            self.trade_id_counter += 1

            current_time = time.time()
            trade = {
                'trade_id': trade_id,
                'symbol': symbol,
                'buy_exchange': buy_exchange,
                'sell_exchange': sell_exchange,
                'open_buy_price': current_buy_price,
                'open_sell_price': current_sell_price,
                'current_spread': current_spread,
                'max_spread': current_spread,
                'open_time': datetime.now(),
                'status': 'open',
                'time_above_threshold': 0.0,
                'last_metrics_update': current_time
            }

            self.open_trades.append(trade)
            self.add_active_trade(symbol, buy_exchange, sell_exchange)  # БЛОКИРУЕМ КОМБИНАЦИЮ
            self.update_trade_counts(trade, 'open')
            # УБРАЛИ ВЫЗОВ buffer_trade_to_csv - открытые сделки не записываем в CSV
            logger.info(f"✅ ОТКРЫТА сделка {trade_id}: {symbol} {buy_exchange}→{sell_exchange} спред: {current_spread:.3f}%")
            return trade_id
            
        except Exception as e:
            logger.error(f"Ошибка открытия сделки: {e}")
            return None


    def close_trade(self, trade_id: str, current_prices: Dict[str, float]):
        """Быстрое закрытие сделки с обновлением счетчиков и снятием блокировки комбинации"""
        for i, trade in enumerate(self.open_trades):
            if trade['trade_id'] == trade_id:
                try:
                    # Перед закрытием делаем финальное обновление метрик
                    self.update_trade_metrics(trade_id, current_prices)
                    
                    buy_price = current_prices.get(trade['buy_exchange'])
                    sell_price = current_prices.get(trade['sell_exchange'])
                    
                    if buy_price and sell_price:
                        trade.update({
                            'close_buy_price': buy_price,
                            'close_sell_price': sell_price,
                            'close_time': datetime.now(),
                            'status': 'closed',
                            'duration_seconds': (datetime.now() - trade['open_time']).total_seconds()
                        })
                        
                        self.calculate_trade_profit_fast(trade)
                    
                    self.closed_trades.append(trade.copy())
                    self.open_trades.pop(i)
                    self.remove_active_trade(trade['symbol'], trade['buy_exchange'], trade['sell_exchange'])  # СНИМАЕМ БЛОКИРОВКУ
                    self.update_trade_counts(trade, 'close')
                    self.buffer_trade_to_csv(trade)  # ЗАПИСЫВАЕМ В CSV ТОЛЬКО ЗАКРЫТЫЕ СДЕЛКИ
                    
                    logger.info(f"🔒 ЗАКРЫТА сделка {trade_id}: {trade['symbol']} {trade['buy_exchange']}→{trade['sell_exchange']}")
                    break
                    
                except Exception as e:
                    logger.error(f"Ошибка закрытия сделки: {e}")
    def calculate_trade_profit_fast(self, trade: Dict):
        """Быстрый расчет прибыли с включением времени выше порога"""
        try:
            open_buy = trade['open_buy_price']
            open_sell = trade['open_sell_price']
            close_buy = trade.get('close_buy_price', open_buy)
            close_sell = trade.get('close_sell_price', open_sell)
            
            gross_profit = (open_sell - open_buy) + (close_buy - close_sell)
            
            # Быстрый расчет комиссий
            fee_multiplier = Config.FUTURES_FEES.get(trade['buy_exchange'], 0.0004)
            total_fees = (open_buy + open_sell + close_buy + close_sell) * fee_multiplier
            
            net_profit = gross_profit - total_fees
            net_profit_percent = (net_profit / open_buy) * 100
            
            # Рассчитываем эффективность по времени
            total_duration = trade.get('duration_seconds', 0)
            time_above_open_threshold = trade.get('time_above_threshold', 0)
            time_above_close_threshold = trade.get('time_above_close_threshold', 0)
            
            efficiency_open = (time_above_open_threshold / total_duration * 100) if total_duration > 0 else 0
            efficiency_close = (time_above_close_threshold / total_duration * 100) if total_duration > 0 else 0
            
            trade.update({
                'gross_profit': gross_profit,
                'total_fees': total_fees,
                'net_profit': net_profit,
                'net_profit_percent': net_profit_percent,
                'time_above_threshold': time_above_open_threshold,
                'time_above_close_threshold': time_above_close_threshold,
                'efficiency_open_percent': efficiency_open,
                'efficiency_close_percent': efficiency_close
            })
            
        except Exception as e:
            logger.error(f"Ошибка расчета прибыли: {e}")

    def buffer_trade_to_csv(self, trade: Dict):
        """Буферизованная запись в CSV ТОЛЬКО для закрытых сделок"""
        try:
            # ЗАПИСЫВАЕМ ТОЛЬКО ЗАКРЫТЫЕ СДЕЛКИ
            if trade.get('status') != 'closed':
                return
                
            # Получаем время выше порога - используем time_above_threshold если есть, иначе 0
            time_above_threshold = trade.get('time_above_threshold', 0)
            
            row = [
                trade['trade_id'],
                trade['symbol'],
                trade['buy_exchange'],
                trade['sell_exchange'],
                self.format_number(trade['open_buy_price']),
                self.format_number(trade['open_sell_price']),
                self.format_number(trade.get('close_buy_price', 0)),
                self.format_number(trade.get('close_sell_price', 0)),
                self.format_number(trade.get('volume', 1.0)),
                self.format_number(trade.get('gross_profit', 0)),
                self.format_number(trade.get('total_fees', 0)),
                self.format_number(trade.get('net_profit', 0)),
                self.format_number(trade.get('net_profit_percent', 0)),
                trade['open_time'].strftime('%Y-%m-%d %H:%M:%S'),
                trade.get('close_time', '').strftime('%Y-%m-%d %H:%M:%S') if trade.get('close_time') else '',
                self.format_number(trade.get('duration_seconds', 0)),
                self.format_number(trade.get('max_spread', 0)),
                self.format_number(time_above_threshold),
                trade['status']
            ]
            
            self._csv_buffer.append(row)
            
            # Периодическая запись буфера
            current_time = time.time()
            if len(self._csv_buffer) >= 10 or current_time - self._last_csv_flush > 5:
                self.flush_csv_buffer()
                    
        except Exception as e:
            logger.error(f"Ошибка буферизации сделки: {e}")

    def flush_csv_buffer(self):
        """Запись буфера в CSV"""
        if not self._csv_buffer:
            return
            
        try:
            with open(self.csv_file, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f, delimiter=';')
                writer.writerows(self._csv_buffer)
            
            self._csv_buffer.clear()
            self._last_csv_flush = time.time()
        except Exception as e:
            logger.error(f"Ошибка записи CSV буфера: {e}")

    def can_open_trade_on_exchanges(self, buy_exchange: str, sell_exchange: str) -> bool:
        """Быстрая проверка лимитов"""
        buy_count = sum(1 for trade in self.open_trades if trade['buy_exchange'] == buy_exchange)
        sell_count = sum(1 for trade in self.open_trades if trade['sell_exchange'] == sell_exchange)
        
        return (buy_count < Config.MAX_TRADES_PER_EXCHANGE and 
                sell_count < Config.MAX_TRADES_PER_EXCHANGE)
    def can_open_trade(self, buy_exchange: str, sell_exchange: str, symbol: str) -> bool:
        """Проверка возможности открытия сделки с учетом всех ограничений"""
        # Проверка общего лимита сделок
        if len(self.open_trades) >= Config.MAX_OPEN_TRADES:
            return False
            
        # Проверка лимита на бирже покупки
        if self.exchange_trade_count[buy_exchange] >= Config.MAX_TRADES_PER_EXCHANGE:
            return False
            
        # Проверка лимита на бирже продажи  
        if self.exchange_trade_count[sell_exchange] >= Config.MAX_TRADES_PER_EXCHANGE:
            return False
            
        # Проверка лимита на символе
        if self.symbol_trade_count[symbol] >= Config.MAX_TRADES_PER_SYMBOL:
            return False
            
        # ПРОВЕРКА КОМБИНАЦИИ: запрет на открытие сделки для той же комбинации символ+биржи
        if self.is_trade_active(symbol, buy_exchange, sell_exchange):
            return False
            
        return True
    def update_trade_counts(self, trade: Dict, operation: str):
        """Обновление счетчиков сделок (operation: 'open' или 'close')"""
        symbol = trade['symbol']
        buy_exchange = trade['buy_exchange']
        sell_exchange = trade['sell_exchange']
        
        if operation == 'open':
            self.exchange_trade_count[buy_exchange] += 1
            self.exchange_trade_count[sell_exchange] += 1
            self.symbol_trade_count[symbol] += 1
        elif operation == 'close':
            self.exchange_trade_count[buy_exchange] = max(0, self.exchange_trade_count[buy_exchange] - 1)
            self.exchange_trade_count[sell_exchange] = max(0, self.exchange_trade_count[sell_exchange] - 1)
            self.symbol_trade_count[symbol] = max(0, self.symbol_trade_count[symbol] - 1)
# ==================== ВЫСОКОЧАСТОТНЫЙ АНАЛИЗАТОР СПРЕДОВ ====================

class HighFrequencySpreadAnalyzer:
    def __init__(self, price_handler: HighFrequencyPriceHandler):
        self.price_handler = price_handler
        
    def get_top_spreads(self, top_n: int = 10) -> List[Dict]:
        """Быстрое получение топ-N спредов"""
        all_spreads = []
        
        for symbol in Config.FUTURES_SYMBOLS:
            symbol_spreads = self.get_symbol_spreads_fast(symbol)
            all_spreads.extend(symbol_spreads)
        
        all_spreads.sort(key=lambda x: x['spread_percent'], reverse=True)
        return all_spreads[:top_n]
    
    def get_symbol_spreads_fast(self, symbol: str) -> List[Dict]:
        """Быстрый расчет спредов для символа"""
        spreads = []
        prices = self.price_handler.get_current_prices(symbol)
        
        if len(prices) < 2:
            return spreads
        
        min_price = min(prices.values())
        max_price = max(prices.values())
        
        if min_price > 0 and max_price > min_price:
            spread_percent = ((max_price - min_price) / min_price) * 100
            
            min_exchange = [k for k, v in prices.items() if v == min_price][0]
            max_exchange = [k for k, v in prices.items() if v == max_price][0]
            
            spreads.append({
                'symbol': symbol,
                'buy_exchange': min_exchange,
                'sell_exchange': max_exchange,
                'buy_price': min_price,
                'sell_price': max_price,
                'spread_percent': spread_percent,
                'spread_usdt': max_price - min_price,
                'timestamp': datetime.now()
            })
        
        return spreads



# ==================== ОПТИМИЗИРОВАННЫЙ DISPLAY MANAGER ====================

class HighFrequencyDisplayManager:
    def __init__(self):
        self.last_display = 0
        self.display_interval = 0.5  # 2 FPS для экономии CPU
        
    async def update_display(self, price_handler: HighFrequencyPriceHandler, 
                        trading_simulator: HighFrequencyTradingSimulator, 
                        top_spreads: List[Dict],
                        all_time_spreads: List[Dict]):
        """Быстрое обновление дисплея с информацией о лимитах"""
        current_time = time.time()
        if current_time - self.last_display < self.display_interval:
            return
            
        self.last_display = current_time
        
        print("\033[H\033[J", end="")
        
        print("⚡ ULTRA HIGH-FREQUENCY FUTURES ARBITRAGE BOT")
        print("=" * 80)
        print(f"📊 Время: {datetime.now().strftime('%H:%M:%S.%f')[:-3]} | "
            f"💰 Сделки: {len(trading_simulator.open_trades)}/{Config.MAX_OPEN_TRADES}")
        print()
        
        # Вывод статуса с информацией о лимитах
        self.print_fast_exchange_status(price_handler, trading_simulator)  # Теперь передаем trading_simulator
        print()
        self.print_fast_open_trades(trading_simulator)
        print()
        self.print_fast_top_spreads(top_spreads)

    def print_fast_exchange_status(self, price_handler):
        """Быстрый вывод статуса бирж"""
        print("🏪 СТАТУС БИРЖ")
        print("-" * 50)
        
        active_count = 0
        line = ""
        
        for i, exchange in enumerate(Config.EXCHANGES):
            active_symbols = len([
                s for s in Config.FUTURES_SYMBOLS 
                if price_handler.is_price_fresh(exchange, s)
            ])
            status = "✅" if active_symbols > 0 else "❌"
            if active_symbols > 0:
                active_count += 1
            line += f"{status} {exchange:8} "
            
            if (i + 1) % 5 == 0:
                print(f"  {line}")
                line = ""
        
        if line:
            print(f"  {line}")
        
        print(f"📈 Активно: {active_count}/{len(Config.EXCHANGES)} бирж")

    def print_fast_open_trades(self, trading_simulator):
        """Быстрый вывод открытых сделок с информацией о блокировках"""
        print("📈 ОТКРЫТЫЕ СДЕЛКИ")
        if not trading_simulator.open_trades:
            print("  Нет открытых сделок")
            return
            
        print(f"{'Symbol':<8} {'Buy→Sell':<16} {'Spread%':<8} {'Duration':<8} {'Locked':<6}")
        print("-" * 55)
        
        for trade in trading_simulator.open_trades[:10]:
            duration = (datetime.now() - trade['open_time']).total_seconds()
            duration_str = f"{duration:.0f}s"
            
            pair = f"{trade['buy_exchange']}→{trade['sell_exchange']}"
            current_spread = trade.get('current_spread', 0)
            
            # Проверяем заблокирована ли комбинация
            is_locked = trading_simulator.is_trade_active(
                trade['symbol'], trade['buy_exchange'], trade['sell_exchange']
            )
            lock_status = "🔒" if is_locked else "⚪"
            
            print(f"{trade['symbol']:<8} {pair:<16} {current_spread:>6.2f}% {duration_str:>8} {lock_status:>6}")

    def print_active_trades(self, trading_simulator):
        """Вывод заблокированных комбинаций"""
        active_trades = trading_simulator.get_active_trades()
        if active_trades:
            print("🔒 ЗАБЛОКИРОВАННЫЕ КОМБИНАЦИИ:")
            for symbol, buy_ex, sell_ex in active_trades:
                print(f"   {symbol} {buy_ex}→{sell_ex}")
        else:
            print("🔓 Нет заблокированных комбинаций")
    def get_active_trades(self) -> List[Tuple[str, str, str]]:
        """Возвращает список всех активных комбинаций"""
        return list(self.active_trade_keys)

    def clear_all_trade_locks(self):
        """Очищает все блокировки (использовать только в аварийных ситуациях)"""
        locked_count = len(self.active_trade_keys)
        self.active_trade_keys.clear()
        logger.warning(f"🧹 Сняты все блокировки ({locked_count} активных комбинаций)")
    def print_fast_top_spreads(self, top_spreads):
        """Быстрый вывод топ спредов"""
        print("🔥 ТОП-5 ТЕКУЩИХ СПРЕДОВ")
        if not top_spreads:
            print("  Нет спредов")
            return
            
        for i, spread in enumerate(top_spreads[:5], 1):
            pair = f"{spread['buy_exchange']}→{spread['sell_exchange']}"
            print(f"{i}. {spread['symbol']:<8} {pair:<16} {spread['spread_percent']:>6.3f}%")
    def print_fast_exchange_status(self, price_handler, trading_simulator):
        """Быстрый вывод статуса бирж с информацией о лимитах"""
        print("🏪 СТАТУС БИРЖ И ЛИМИТЫ")
        print("-" * 60)
        
        active_count = 0
        line = ""
        
        for i, exchange in enumerate(Config.EXCHANGES):
            active_symbols = len([
                s for s in Config.FUTURES_SYMBOLS 
                if price_handler.is_price_fresh(exchange, s)
            ])
            
            trade_count = trading_simulator.exchange_trade_count.get(exchange, 0)
            status = "✅" if active_symbols > 0 else "❌"
            limit_status = f"({trade_count}/{Config.MAX_TRADES_PER_EXCHANGE})"
            
            if active_symbols > 0:
                active_count += 1
                
            line += f"{status} {exchange:8} {limit_status} "
            
            if (i + 1) % 4 == 0:  # Уменьшаем до 4 в строке из-за добавленной информации
                print(f"  {line}")
                line = ""
        
        if line:
            print(f"  {line}")
        
        print(f"📈 Активно: {active_count}/{len(Config.EXCHANGES)} бирж")
        print(f"📊 Открыто сделок: {len(trading_simulator.open_trades)}/{Config.MAX_OPEN_TRADES}")
        
        # Вывод статистики по символам
        symbol_stats = []
        for symbol in Config.FUTURES_SYMBOLS:
            count = trading_simulator.symbol_trade_count.get(symbol, 0)
            if count > 0:
                symbol_stats.append(f"{symbol}:{count}")
        
        if symbol_stats:
            print(f"🎯 Сделки по символам: {', '.join(symbol_stats)}")
# ==================== ВЫСОКОЧАСТОТНЫЙ ОСНОВНОЙ БОТ ====================

class HighFrequencyFuturesArbitrageBot:
    def __init__(self):
        self.price_handler = HighFrequencyPriceHandler()
        self.websocket_manager = WebSocketManager(self.price_handler)
        self.arbitrage_calculator = HighFrequencyArbitrageCalculator(self.price_handler)
        self.trading_simulator = HighFrequencyTradingSimulator()
        self.spread_analyzer = HighFrequencySpreadAnalyzer(self.price_handler)
        self.display_manager = HighFrequencyDisplayManager()
        self.is_running = False
        
        # Высокочастотные метрики
        self.iteration_count = 0
        self.last_cleanup = time.time()
        self.all_time_best_spreads = []
        self.last_display_update = 0
        self.last_connection_check = time.time()

    async def start(self):
        """Запуск высокочастотного бота"""
        self.is_running = True
        await self.websocket_manager.start()
        
        # Запускаем высокочастотный основной цикл
        await self.high_frequency_main_loop()

    async def high_frequency_main_loop(self):
        """УЛУЧШЕННЫЙ основной цикл с защитой от ошибок"""
        logger.info("🚀 Starting IMPROVED HIGH-FREQUENCY arbitrage bot...")
        
        last_aggressive_reconnect = time.time()
        error_count = 0
        max_errors = 10
        
        while self.is_running:
            try:
                self.iteration_count += 1
                current_time = time.time()
                
                # СБРАСЫВАЕМ СЧЕТЧИК ОШИБОК ПРИ УСПЕШНЫХ ИТЕРАЦИЯХ
                if error_count > 0:
                    error_count -= 0.1  # Постепенно уменьшаем счетчик
                
                # Быстрый поиск возможностей
                opportunities = self.arbitrage_calculator.find_opportunities()
                
                # Быстрое управление сделками
                await self.fast_trade_management(opportunities)
                
                # Быстрое обновление метрик
                await self.fast_update_metrics()
                
                # Периодическое обновление дисплея
                if current_time - self.last_display_update > 0.5:
                    await self.fast_display_update(opportunities)
                    self.last_display_update = current_time
                
                # ПЕРИОДИЧЕСКАЯ ПРОВЕРКА ПОДКЛЮЧЕНИЙ
                if current_time - self.last_connection_check > 30:
                    await self.websocket_manager.check_and_reconnect_failed_exchanges()
                    self.last_connection_check = current_time
                
                # Периодическая очистка
                if current_time - self.last_cleanup > 10:
                    await self.fast_cleanup()
                    self.last_cleanup = current_time
                
                # Короткая пауза
                await asyncio.sleep(0.001)
                
            except Exception as e:
                error_count += 1
                logger.error(f"❌ High-frequency loop error (count: {error_count}): {e}")
                
                # ЕСЛИ СЛИШКОМ МНОГО ОШИБОК - ДЕЛАЕМ ПАУЗУ
                if error_count >= max_errors:
                    logger.error(f"🚨 TOO MANY ERRORS ({error_count}), pausing for 30 seconds...")
                    await asyncio.sleep(30)
                    error_count = max_errors / 2  # Сбрасываем счетчик наполовину
                
                await asyncio.sleep(0.1)  # Пауза после ошибки

    async def fast_trade_management(self, opportunities: List[Dict]):
        """Быстрое управление сделками с проверкой целостности блокировок"""
        # Быстрое закрытие сделок
        for trade in self.trading_simulator.open_trades[:]:
            if trade.get('current_spread', 100) <= Config.CLOSE_SPREAD_THRESHOLD:
                symbol_prices = self.price_handler.get_current_prices(trade['symbol'])
                self.trading_simulator.close_trade(trade['trade_id'], symbol_prices)
        
        # Проверка целостности: каждая открытая сделка должна быть в active_trade_keys
        for trade in self.trading_simulator.open_trades:
            if not self.trading_simulator.is_trade_active(trade['symbol'], trade['buy_exchange'], trade['sell_exchange']):
                logger.warning(f"⚠️ Нарушение целостности: сделка {trade['trade_id']} не в active_trade_keys")
                # Восстанавливаем блокировку
                self.trading_simulator.add_active_trade(trade['symbol'], trade['buy_exchange'], trade['sell_exchange'])
        
        # Быстрое открытие новых сделок
        if len(self.trading_simulator.open_trades) < Config.MAX_OPEN_TRADES:
            for opportunity in opportunities[:5]:
                if len(self.trading_simulator.open_trades) >= Config.MAX_OPEN_TRADES:
                    break
                    
                current_prices = self.price_handler.get_current_prices(opportunity['symbol'])
                trade_id = self.trading_simulator.open_trade(opportunity, current_prices)
                if trade_id:
                    await asyncio.sleep(0.0001)

    async def fast_update_metrics(self):
        """Быстрое обновление метрик открытых сделок"""
        for trade in self.trading_simulator.open_trades:
            prices = self.price_handler.get_current_prices(trade['symbol'])
            if prices:
                self.trading_simulator.update_trade_metrics(trade['trade_id'], prices)

    async def fast_display_update(self, opportunities: List[Dict]):
        """Быстрое обновление дисплея"""
        try:
            top_spreads = self.spread_analyzer.get_top_spreads(10)
            await self.update_all_time_best_spreads(top_spreads)
            await self.display_manager.update_display(
                self.price_handler, 
                self.trading_simulator, 
                top_spreads,
                self.all_time_best_spreads
            )
        except Exception as e:
            logger.error(f"Ошибка обновления дисплея: {e}")

    async def fast_cleanup(self):
        """Быстрая очистка старых данных"""
        try:
            # Очищаем старые спреды
            current_time = time.time()
            self.all_time_best_spreads = [
                spread for spread in self.all_time_best_spreads
                if current_time - spread.get('discovery_time', 0) < 3600
            ]
            
            # Сбрасываем CSV буфер
            self.trading_simulator.flush_csv_buffer()
            
        except Exception:
            pass

    async def update_all_time_best_spreads(self, top_spreads):
        """Обновление лучших спредов за все время"""
        try:
            current_time = time.time()
            # Добавляем временные метки к новым спредам
            for spread in top_spreads:
                spread['discovery_time'] = current_time
            
            # Объединяем с существующими
            self.all_time_best_spreads.extend(top_spreads)
            
            # Сортируем по убыванию спреда и оставляем топ-20
            self.all_time_best_spreads.sort(key=lambda x: x['spread_percent'], reverse=True)
            self.all_time_best_spreads = self.all_time_best_spreads[:20]
            
        except Exception as e:
            logger.error(f"Error updating all-time spreads: {e}")

    async def stop(self):
        """Быстрая остановка"""
        self.is_running = False
        self.trading_simulator.flush_csv_buffer()
        await self.websocket_manager.stop()

# ==================== ЗАПУСК ====================

async def main():
    bot = HighFrequencyFuturesArbitrageBot()
    try:
        await bot.start()
    except KeyboardInterrupt:
        await bot.stop()
    except Exception as e:
        print(f"Критическая ошибка: {e}")
        await bot.stop()

if __name__ == "__main__":
    print("🚀 ULTRA HIGH-FREQUENCY ARBITRAGE BOT - МАКСИМАЛЬНАЯ СКОРОСТЬ")
    asyncio.run(main())