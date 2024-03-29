import 'dart:async';
import 'dart:math';

import 'package:fireblend/fireblend.dart';
import 'package:rxdart/rxdart.dart';

/// None of the methods in these classes should be called by themselves.
/// The idea behind these is to implement the abstract methods and then
/// pass the instance of the class as an input to a [FireblendStreamProvider]

class _EventType {
  static const String VALUE = "value";
  static const String CHILD_ADDED = "child_added";
  static const String CHILD_CHANGED = "child_changed";
  static const String CHILD_REMOVED = "child_removed";
}

abstract class ChildStreamElement<T> extends FireblendStreamElement<T> {
  ChildStreamElement(List<FireblendQuery> queries)
      : super(queries, [
          _EventType.CHILD_ADDED,
          _EventType.CHILD_CHANGED,
          _EventType.CHILD_REMOVED
        ]);

  @override
  Stream<Map<String, T>> get state =>
      _stateController.stream as Stream<Map<String, T>>;

  @override
  Map<String, T>? currentState() => _state;
}

abstract class ValueStreamElement<T> extends FireblendStreamElement<T> {
  ValueStreamElement(List<FireblendQuery> queries)
      : super(queries, [_EventType.VALUE]);

  Future<MapEntry<String, T>?> converterSync(FireblendDataSnapshot snapshot);

  @override
  Stream<MapEntry<String, T>?> get state =>
      _stateController.stream.map((state) =>
          (state?.entries?.isNotEmpty ?? false) ? state.entries.first : null);

  @override
  MapEntry<String, T>? currentState() =>
      (_state?.entries.isNotEmpty ?? false) ? _state?.entries.first : null;

  Future<List?> once() async {
    List<Future> futures = [];
    for (FireblendQuery query in _queries) {
      await query.once().then((FireblendDataSnapshot? snapshot) {
        if (snapshot?.value != null) futures.add(converterSync(snapshot!));
      });
    }
    return await Future.wait(futures);
  }
}

abstract class FireblendStreamElement<T> extends FireblendElement<T> {
  final _stateController = BehaviorSubject();
  StreamController<MapEntry<String, T>> _addController =
      StreamController.broadcast();
  StreamController<MapEntry<String, T>> _modifyController =
      StreamController.broadcast();
  StreamController<String> _removeController = StreamController.broadcast();

  FireblendStreamElement(List<FireblendQuery> queries, List<String> eventTypes)
      : super(queries, eventTypes) {
    _stateController.add(_state);
  }

  Stream<dynamic> get state;

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
  void _onRemoved(MapEntry<String, T?> entry) {
    _stateController.add(_state);
    _removeController.add(entry.key);
  }

  @override
  Future close() async {
    await super.close();
    List<Future> futures = [];
    futures.add(_stateController.close());
    futures.add(_addController.close());
    futures.add(_modifyController.close());
    futures.add(_removeController.close());
    await Future.wait(futures);
  }
}

abstract class FireblendElement<T> {
  final List<FireblendQuery> _queries;
  final List<String> _eventTypes;
  late Map<String, T>? _state;
  late bool _closed;
  late bool _ready;

  late Map<String, Set<String>> _mapping;
  late Map<String, Set<String>> _sources;
  late Map<String, StreamSubscription> _subscriptions;

  FireblendElement(this._queries, this._eventTypes) {
    _state = Map();
    _closed = false;
    _ready = false;
    _mapping = Map();
    _sources = Map();
    _subscriptions = Map();
  }

  dynamic currentState();

  void readyListeners() {
    if (_ready) return;
    _ready = true;
    for (FireblendQuery query in _queries) {
      for (String type in _eventTypes) {
        switch (type) {
          case _EventType.VALUE:
            _subscribe(query.onValue?.listen((FireblendEvent? event) {
              if (!_closed && event?.snapshot?.value != null)
                converterAsync(_id(query.hashCode, event?.snapshot?.key ?? ''),
                    event?.snapshot);
              if (!_closed && event?.snapshot?.value == null)
                _remover(_id(query.hashCode, event?.snapshot?.key ?? ''));
            }), _id(query.hashCode, _EventType.VALUE));
            break;
          case _EventType.CHILD_ADDED:
            _subscribe(query.onChildAdded?.listen((FireblendEvent? event) {
              if (!_closed && event?.snapshot?.value != null)
                converterAsync(_id(query.hashCode, event?.snapshot?.key ?? ''),
                    event?.snapshot);
            }), _id(query.hashCode, _EventType.CHILD_ADDED));
            break;
          case _EventType.CHILD_CHANGED:
            _subscribe(query.onChildChanged?.listen((FireblendEvent? event) {
              if (!_closed && event?.snapshot?.value != null)
                converterAsync(_id(query.hashCode, event?.snapshot?.key ?? ''),
                    event?.snapshot);
            }), _id(query.hashCode, _EventType.CHILD_CHANGED));
            break;
          case _EventType.CHILD_REMOVED:
            _subscribe(query.onChildRemoved?.listen((FireblendEvent? event) {
              if (!_closed)
                _remover(_id(query.hashCode, event?.snapshot?.key ?? ''));
            }), _id(query.hashCode, _EventType.CHILD_REMOVED));
            break;
          default:
            throw Exception("Unsupported event type.");
        }
      }
    }
  }

  String _id(int hash, String key) => hash.toString() + " (" + key + ")";

  /// This method must only be called from within [converterAsync].
  /// It inserts a new [entry] into the [state].
  /// The [source] must be the one that came as the
  /// parameter of the aforementioned [converterAsync].
  bool insert(String? source, MapEntry<String, T> entry) {
    if (_closed) return false;
    bool contained = _state?.containsKey(entry.key) ?? false;
    _state?.addEntries([entry]);
    if (contained)
      _onModified(entry);
    else
      _onAdded(entry);
    // Not adding a source is only justified when the entry is
    // being updated from somewhere other than one of its sources.
    if (source == null) return true;
    if (_mapping[source] == null) _mapping[source] = Set();
    _mapping[source]?.add(entry.key);
    if (_sources[entry.key] == null) _sources[entry.key] = Set();
    _sources[entry.key]?.add(source);
    return true;
  }

  /// This method must only be called from within [converterAsync].
  /// It tries to remove an existing [entry] from the [state].
  /// The [source] must be the one that came as the
  /// parameter of the aforementioned [converterAsync].
  bool extract(String? source, MapEntry<String, T> entry) {
    if (_closed || source == null) return false;
    if (!(_state?.containsKey(entry.key) ?? false)) return false;
    if (_mapping[source] != null) _mapping[source]?.remove(entry.key);
    if (_sources[entry.key] != null) {
      if (_sources[entry.key]!.remove(source)) {
        if (_sources[entry.key]!.isEmpty) {
          _state?.remove(entry.key);
          _onRemoved(entry);
          return true;
        }
      }
    }
    return false;
  }

  /// This method must only be called from within [converterAsync].
  /// It ties the [subscription] to the lifecycle of this class.
  /// The [source] must be the one that came as the
  /// parameter of the aforementioned [converterAsync].
  /// The [key] uniquely identifies the [subscription].
  void subscribe(String source, StreamSubscription subscription,
      {String? key}) {
    // Not providing a key means this subscription will effectively
    // be in place as long as the source is present.
    if (key == null) key = Random(42).nextDouble().toString();
    _subscribe(subscription, key);
    if (!_closed) {
      if (_mapping[source] == null) _mapping[source] = Set();
      _mapping[source]?.add(key);
    }
  }

  /// This method should only be called from within [converterAsync].
  bool isSubscribed(String key) => _subscriptions.containsKey(key);

  /// This method should only be called from within [converterAsync].
  Future cancelSubscription(String key) async {
    await _subscriptions[key]?.cancel();
    _subscriptions.remove(key);
  }

  void _subscribe(StreamSubscription? subscription, String key) {
    if (_closed)
      subscription?.cancel();
    else {
      _subscriptions[key]?.cancel();
      if (subscription != null) _subscriptions[key] = subscription;
    }
  }

  /// Update the [_state] by using the methods [insert] and [subscribe]
  void converterAsync(String source, FireblendDataSnapshot? snapshot);

  void _remover(String source) {
    if (_mapping.containsKey(source)) {
      for (String key in _mapping[source]!) {
        // Cancel related subscriptions.
        _subscriptions[key]?.cancel();
        dynamic result = _subscriptions.remove(key);
        // Delete related entries.
        if (result == null) {
          result = _sources[key]?.remove(source);
          if (result ?? false) {
            if (_sources[key]?.isEmpty ?? false) {
              T? value = _state?[key];
              _state?.remove(key);
              _onRemoved(MapEntry(key, value));
            }
          }
        }
      }
      _mapping.remove(source);
    }
  }

  void _onAdded(MapEntry<String, T> entry);

  void _onModified(MapEntry<String, T> entry);

  void _onRemoved(MapEntry<String, T?> entry);

  /// Cancels all of the [StreamSubscription] inside of [_subscriptions]
  /// that were tied to class through the use of [subscribe].
  Future close() async {
    _closed = true;
    List<Future> futures = [];
    for (String key in _subscriptions.keys)
      if (_subscriptions[key] != null)
        futures.add(_subscriptions[key]!.cancel());
    _subscriptions.clear();
    await Future.wait(futures);
  }

  Future clear() async {
    _mapping.clear();
    _sources.clear();
    for (String key in _state?.keys ?? {}) {
      _onRemoved(MapEntry(key, _state![key]!));
    }
    _state?.clear();
  }
}
