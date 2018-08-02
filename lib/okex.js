
const WebSocket = require('ws');
const crypto = require('crypto');
const md5 = crypto.createHash('md5');

class Okex {

	constructor(options) {
		if (!options) throw 'option requires';
		if (!options.apiKey || !options.secretKey) throw 'option require apiKey and secretKey';

		this.options = options;
		this.wss = 'wss://real.okex.com:10440/websocket/okexapi' || options.wss;
		this.apiKey = options.apiKey;
		this.secretKey = options.secretKey;

		this.debugOpt = options.debug || false;

		this.subscribeChannels = [];
		this.channelHandlers = {};
		this.handler = ()=>{};
	}

	start() {
		this.checkRunning();
		this._running = true;
		this._init();
	}

	debug() {
		if (this.debugOpt) {
			var args = [new Date(), 'OKEX-DEBUG'];
			for (var i=0; i<arguments.length; i++) args.push(arguments[i]);
			console.log.apply(console, args);
		}
	}

	/**
	 *
	 * @symbol: btc, eth, eos
	 * @period: this_week, next_week, quarter
	 * @time 1min, 3min, 5min, 15min, 30min, 1hour, 2hour, 4hour, 6hour, 12hour, day, 3day, week
	 */
	onFutureKline(symbol, period, time, handler) {
		var channel = 'ok_sub_futureusd_'+symbol+'_kline_'+period+'_'+time;
		this._addChannel(channel);
		this._registerChannelHandler(channel, handler);
	}

	/**
	 *
	 * @symbol: btc, eth, eos
	 * @period: this_week, next_week, quarter
	 * 
	 */
	onFutureDepth(symbol, period, handler) {
		var channel = 'ok_sub_futureusd_'+symbol+'_depth_'+period;
		this._addChannel(channel);
		this._registerChannelHandler(channel, handler);
	}

	/**
	 *
	 * @symbol: btc, eth, eos
	 * @period: this_week, next_week, quarter
	 * 
	 */
	onFutureTrade(symbol, period, handler) {
		var channel = 'ok_sub_futureusd_'+symbol+'_trade_'+period;
		this._addChannel(channel);
		this._registerChannelHandler(channel, handler);
	}

	/**
	 *
	 * @symbol: btc, eth, eos
	 * @period: this_week, next_week, quarter
	 * 
	 */
	onFutureTicker(symbol, period, handler) {
		var channel = 'ok_sub_futureusd_'+symbol+'_ticker_'+period;
		this._addChannel(channel);
		this._registerChannelHandler(channel, handler);
	}

	/**
	 *
	 * @symbol: btc, eth, eos
	 * @period: this_week, next_week, quarter
	 * 
	 */
	onFutureIndex(symbol, handler) {
		var channel = 'ok_sub_futureusd_'+symbol+'_index';
		this._addChannel(channel);
		this._registerChannelHandler(channel, handler);
	}

	onMyTrades(handler) {
		var channel = 'ok_sub_futureusd_trades';
		this._addChannel(channel);
		this._registerChannelHandler(channel, handler);
	}

	onMyPositions(handler) {
		var channel = 'ok_sub_futureusd_positions';
		this._addChannel(channel);
		this._registerChannelHandler(channel, handler);
	}

	onMyInfo(handler) {
		var channel = 'ok_sub_futureusd_userinfo';
		this._addChannel(channel);
		this._registerChannelHandler(channel, handler);
	}

	setOrderHandler(handler) {
		var channel = 'ok_futureusd_trade';
		this._addChannel(channel);
		this._registerChannelHandler(channel, handler);
	}
	setCancleOrderHandler(handler) {
		var channel = 'ok_futureusd_cancel_order';
		this._addChannel(channel);
		this._registerChannelHandler(channel, handler);
	}

	/**
	 * order parameters
	 * @symbol btc_usd
	 * @contract_type this_week/next_week/quarter
	 * @amount	amount
	 * @price	
	 * @match_price
	 * @lever_rate
	 *
	 */
	doFutureOrder(order) {
		var channel = 'ok_futureusd_trade';
		var paramters = {};
		this._sign(paramters);
		this._send({event: 'addChannel', channel: channel, parameters: parameters});
	}

	doCancelFutureOrder(id, symbol, contract_type) {
		var channel = 'ok_futureusd_trade';
		var paramters = {};
		this._sign(paramters);
		this._send({event: 'addChannel', channel: channel, parameters: parameters});
	}


	_addChannel(channel) {
		this.checkRunning();
		this.subscribeChannels.push(channel);
		this.debug('subscribe new channel', channel);
	}

	_registerChannelHandler(channel, handler) {
		if (!this.channelHandlers[channel]) {
			this.channelHandlers[channel] = [];
		}
		this.channelHandlers[channel].push(handler);
		this.debug('register channel handler', channel);
	}

	_onEvent(event) {
		//this.debug('onEvent', event);
		// global handler
		//this.handler(event);
		var channel = this._parseChannel(event);
        var fn = this.channelHandlers[channel]; //this will allow to use handlers
        this.debug('onMessage', channel, event);
        if (fn) {
            fn[0](event); //this will allow to use handlers
        } else {
			console.log('ignore unknown channel', channel, event);
		}
	}

	_parseChannel(event) {
		var channel = event.channel;
		if (channel == 'addChannel') {
			channel = event.data.channel;		
		}
		return channel;
	}

	_init() {
		/// init login handler
		this._registerChannelHandler('pong', (event) => {
			this.pingPending = false;	// mark ping is OK
			setTimeout(this._ping.bind(this), 27*1000);
		});
		var self = this;
		this._registerChannelHandler('login', (event) => {
			this.debug('handle login event', event);
			if (event.data && event.data.result) {
				self._afterLogin();
			} else {
				console.log('ERROR: Login Failed', event);
				throw new Error('Login Failed');
			}
		});
		/// init channel handlers
		this._initHandlers();
		const ws = new WebSocket(this.wss, {perMessageDeflate: false });
		this.ws = ws;
		var self = this;
		ws.on('message', (text) => {
			// convert as json first?
			var data = JSON.parse(text);
			//self._onEvent(data);
			if (data.length) {
				data.forEach((x) => {
					self._onEvent(x);
				});
			} else if (data.channel) {
				self._onEvent(data);
			} else {
				// not event response, ignore
				self.debug('ERROR', 'Invalid websocket message', text);
			}
		});
		ws.on('open', () => {
			// login first
			this._login();
		});
	}

	_initHandlers() {
		var fns = {};
		for (var k in this.channelHandlers) {
			var handlers = this.channelHandlers[k];	/// should copy avoid changed?
			var fn = (x) => {
				this.debug('handler function', k, handlers.length);
				handlers.forEach((f) => {
					f(x);
				});
			};
			fns[k] = fn;
		}
		this._handlers = fns;
	}

	close() {
		if (this.ws) {
			this.ws.close();
			this.ws = null;
		}
	}

	_send(obj) {
		this.debug('send message', obj);
		if (typeof obj == 'string') {
			this.ws.send(obj);
		} else {
			this.ws.send(JSON.stringify(obj));
		}
	}

	_ping() {
		// ready next pong
		this.pingPending = true;
		this._send({event: 'ping'});
	}

	// calc sign, and append api_key and sign
	_sign(data) {
		var query = '';
		data.api_key = this.apiKey;
		var keys= Object.keys(data).sort();
		for (var i in keys) {
			var key = keys[i];
			// unescape ???
			query += key+'='+data[key]+'&';
		}
		query += 'secret_key='+this.secretKey;
		md5.update(query, 'utf8');
		var sign = md5.digest('hex').toUpperCase();
		//this.debug('sign=', sign, 'RAW=', query);
		data.sign = sign;
		return data;
	}

	_login() {
		var data = this._sign({api_key: this.apiKey});
		this._send({event: 'login', parameters: data});
	}

	_afterLogin() {
		// register all channels
		var hash = {};
		var channels = [];
		this.subscribeChannels.forEach((x) => {
			if (hash[x]) return;	// ignore if avoid repeat sub
			hash[x] = true;
			//channels.push({'event':'addChannel','channel':x});
			this._send({'event':'addChannel','channel':x})
		});
		// batch subscribe
		//this._send(channels);
		// start ping 
		this._ping();
	}

	checkRunning() {
		if (this._running) throw 'should NOT be here when running';
	}

}

module.exports = Okex;

