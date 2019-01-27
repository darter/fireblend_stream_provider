import 'dart:async';
import 'dart:math';

import 'package:fireblend/fireblend.dart';
import 'package:rxdart/rxdart.dart';

class _EventType {
  static const String VALUE = "value";
  static const String CHILD_ADDED = "child_added";
  static const String CHILD_CHANGED = "child_changed";
  static const String CHILD_REMOVED = "child_removed";
  static const String CHILD_MOVED = "child_moved";
}

abstract class ChildStreamElement<T> extends FireblendStreamElement<T> {

  ChildStreamElement(List<FireblendQuery> queries)
      : super(queries, [_EventType.CHILD_ADDED, _EventType.CHILD_CHANGED, _EventType.CHILD_REMOVED]);

  @override
  Observable<Map<String, T>> get state => _stateController.stream;

  @override
  Map<String, T> currentState() => _state;
}

abstract class ValueStreamElement<T> extends FireblendStreamElement<T> {

  ValueStreamElement(List<FireblendQuery> queries)
      : super(queries, [_EventType.VALUE]);

  Future<MapEntry<String, T>> converterSync(FireblendDataSnapshot snapshot);

  @override
  Observable<MapEntry<String, T>> get state => _stateController.stream
      .map((state) => (state?.entries?.isNotEmpty ?? false) ? state.entries.first : null);

  @override
  MapEntry<String, T> currentState() =>
      (_state?.entries?.isNotEmpty ?? false) ? _state.entries.first : null;

  Future<List> once() async {
    List<Future> futures = List();
    for (FireblendQuery query in _queries) {
      await query.once().then((FireblendDataSnapshot snapshot) {
        if (snapshot.value != null)
          futures.add(converterSync(snapshot));
      });
    } return await Future.wait(futures);
  }
}

abstract class FireblendStreamElement<T> extends FireblendElement<T> {
  BehaviorSubject<Map<String, T>> _stateController = BehaviorSubject();
  StreamController<MapEntry<String, T>> _addController = StreamController.broadcast();
  StreamController<MapEntry<String, T>> _modifyController = StreamController.broadcast();
  StreamController<String> _removeController = StreamController.broadcast();

  FireblendStreamElement(List<FireblendQuery> queries, List<String> eventTypes)
      : super(queries, eventTypes) {
    _stateController.add(_state);
  }

  Observable<dynamic> get state;

  Stream<MapEntry<String, T>> get added => _addController.stream;

  Stream<MapEntry<String, T>> get modified => _modifyController.stream;

  Stream<String> get removed => _removeController.stream;

  @override
  void _onAdded(MapEntry<String, T> entry) {
    _stateController.add(_state);
    _addController.add(entry);
  }

  @override
  void _onModified(MapEntry<String, T> entry) {
    _stateController.add(_state);
    _modifyController.add(entry);
  }

  @override
  void _onRemoved(MapEntry<String, T> entry) {
    _stateController.add(_state);
    _removeController.add(entry.key);
  }

  @override
  Future close() async {
    await super.close();
    List<Future> futures = List();
    futures.add(_stateController.close());
    futures.add(_addController.close());
    futures.add(_modifyController.close());
    futures.add(_removeController.close());
    await Future.wait(futures);
  }
}

abstract class FireblendElement<T> {
  List<FireblendQuery> _queries;
  List<String> eventTypes;
  Map<String, T> _state;
  bool _closed;
  bool _ready;

  Map<String, Set<String>> _mapping;
  Map<String, StreamSubscription> _subscriptions;

  FireblendElement(this._queries, this.eventTypes) {
    _state = Map();
    _closed = false;
    _ready = false;
    _mapping = Map();
    _subscriptions = Map();
  }

  dynamic currentState();

  void readyListeners() {
    if (_ready) return;
    _ready = true;
    for (FireblendQuery query in _queries) {
      for (String type in eventTypes) {
        switch (type) {
          case _EventType.VALUE:
            _subscribe(query.onValue.listen((FireblendEvent event) {
              if (!_closed && event.snapshot.value != null)
                converterAsync(event.snapshot);
              if (!_closed && event.snapshot.value == null)
                _remover(event.snapshot.key);
            }), _EventType.VALUE); break;
          case _EventType.CHILD_ADDED:
            _subscribe(query.onChildAdded.listen((FireblendEvent event) {
              if (!_closed && event.snapshot.value != null)
                converterAsync(event.snapshot);
            }), _EventType.CHILD_ADDED); break;
          case _EventType.CHILD_CHANGED:
            _subscribe(query.onChildChanged.listen((FireblendEvent event) {
              if (!_closed && event.snapshot.value != null)
                converterAsync(event.snapshot);
            }), _EventType.CHILD_CHANGED); break;
          case _EventType.CHILD_REMOVED:
            _subscribe(query.onChildRemoved.listen((FireblendEvent event) {
              if (!_closed) _remover(event.snapshot.key);
            }), _EventType.CHILD_REMOVED); break;
          default:
            throw Exception("Unsupported event type.");
        }
      }
    }
  }

  bool insert(String source, MapEntry<String, T> entry) {
    if (_closed) return false;
    bool contained = _state.containsKey(entry.key);
    _state.addEntries([entry]);
    if (_mapping[source] == null)
      _mapping[source] = Set();
    _mapping[source].add(entry.key);
    if (contained) _onModified(entry);
    else _onAdded(entry);
    return true;
  }

  void subscribe(String source, StreamSubscription subscription, {String key}) {
    if (key == null)
      key = Random().nextDouble().toString();
    if (key == _EventType.VALUE
        || key == _EventType.CHILD_ADDED
        || key == _EventType.CHILD_CHANGED
        || key == _EventType.CHILD_REMOVED
        || key == _EventType.CHILD_MOVED)
      throw Exception("The associated key must not match any event type.");
    _subscribe(subscription, key);
    if (!_closed) {
      if (_mapping[source] == null)
        _mapping[source] = Set();
      _mapping[source].add(key);
    }
  }

  void _subscribe(StreamSubscription subscription, String key) {
    if (_closed)
      subscription.cancel();
    else {
      _subscriptions[key]?.cancel();
      _subscriptions[key] = subscription;
    }
  }

  void converterAsync(FireblendDataSnapshot snapshot);

  void _remover(String source) {
    if (_mapping.containsKey(source)) {
      for (String key in _mapping[source]) {
        // Cancel related subscriptions.
        _subscriptions[key]?.cancel();
        dynamic result = _subscriptions.remove(key);
        // Delete related entries.
        if (result == null) {
          T value = _state[key];
          _state.remove(key);
          _onRemoved(MapEntry(key, value));
        }
      } _mapping.remove(source);
    }
  }

  void _onAdded(MapEntry<String, T> entry);

  void _onModified(MapEntry<String, T> entry);

  void _onRemoved(MapEntry<String, T> entry);

  Future close() async {
    List<Future> futures = List();
    for (String key in _subscriptions.keys)
      futures.add(_subscriptions[key].cancel());
    _subscriptions.clear();
    await Future.wait(futures);
  }
}