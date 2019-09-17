import 'dart:async';
import 'dart:math';

import 'package:rxdart/rxdart.dart';

import 'fireblend_element.dart';

class CollectionStream<T> {
  final CollectionStreamProvider<T> _provider;

  CollectionStream._(this._provider);

  Observable<Map<String, T>> get state => _provider.state;

  Stream<MapEntry<String, T>> get addition => _provider.addition;

  Stream<MapEntry<String, T>> get modification => _provider.modification;

  Stream<String> get removal => _provider.removal;
}

class ValueStream<T> {
  final ValueStreamProvider<T> _provider;

  ValueStream._(this._provider);

  Observable<MapEntry<String, T>> get state => _provider.state;
}

class CollectionStreamProvider<T> extends FireblendStreamProvider<T> {
  Map<String, T> _state;
  Map<String, T> _inserted;
  bool Function(String, T) _filter;
  Map<String, Set<String>> _sources;
  BehaviorSubject<Map<String, T>> _stateController;
  StreamController<MapEntry<String, T>> _additionController;
  StreamController<MapEntry<String, T>> _modificationController;
  StreamController<String> _removalController;
  bool _filterInserted;

  CollectionStreamProvider({bool filterInserted = true}) {
    _state = Map();
    _inserted = Map();
    _sources = Map();
    _stateController = BehaviorSubject<Map<String, T>>();
    _additionController = StreamController<MapEntry<String, T>>.broadcast();
    _modificationController = StreamController<MapEntry<String, T>>.broadcast();
    _removalController = StreamController<String>.broadcast();
    _stateController.add(Map());
    _filterInserted = filterInserted;
  }

  CollectionStream<T> get readable => CollectionStream._(this);

  Observable<Map<String, T>> get state => _stateController.stream.map((state) {
    if (_filter == null) return Map.from(state);
    else return Map.from(state)..removeWhere((key, value) => !_filter(key, value)
        && (_filterInserted || (!_filterInserted && !_inserted.containsKey(key))));
  });

  Stream<MapEntry<String, T>> get addition => _additionController.stream;

  Stream<MapEntry<String, T>> get modification => _modificationController.stream;

  Stream<String> get removal =>  _removalController.stream;

  Map<String, T> currentState() {
    if (_filter == null) return Map.from(_state);
    else return Map.from(_state)
      ..removeWhere((key, value) => !_filter(key, value));
  }

  void insert(MapEntry<String, T> entry) {
    if (_closed)
      throw Exception("The fireblend stream provider has already been closed.");
    bool contained = _state.containsKey(entry.key);
    _state.addEntries([entry]);
    _inserted.addEntries([entry]);
    _stateController.add(_state);
    if (_filter != null && _filterInserted) {
      if (_filter(entry.key, entry.value)) {
        if (contained) _modificationController.add(entry);
        else _additionController.add(entry);
      }
    } else {
      if (contained) _modificationController.add(entry);
      else _additionController.add(entry);
    }
  }

  void remove(String key) {
    if (_closed)
      throw Exception("The fireblend stream provider has already been closed.");
    T removed = _inserted.remove(key);

    if (_filter != null) {
      if (removed != null  && (!_filterInserted || _filter(key, removed))) {
        if (!_sources.containsKey(key) || (_sources[key]?.isEmpty ?? true)) {
          _state.remove(key);
          _stateController.add(_state);
          _removalController.add(key);
        } else if (!_filter(key, removed))
          _removalController.add(key);
      }
    } else if (removed != null){
      if (!_sources.containsKey(key)
          || (_sources[key]?.isEmpty ?? true)) {
        _state.remove(key);
        _stateController.add(_state);
        _removalController.add(key);
      }
    }
  }

  void addFilter(bool Function(String, T) filter) {
    if (_closed)
      throw Exception("The fireblend stream provider has already been closed.");
    if (filter == null) return deleteFilter();
    bool Function(String, T) oldFilter = _filter;
    _filter = filter;
    _stateController.add(_state);
    if (oldFilter != null) {
      for (MapEntry<String, T> entry in _state.entries) {
        if ((_filterInserted || (!_filterInserted
            && !_inserted.containsKey(entry.key)))) {
          if (!oldFilter(entry.key, entry.value)
              && _filter(entry.key, entry.value))
            _additionController.add(entry);
          else if (oldFilter(entry.key, entry.value)
              && !_filter(entry.key, entry.value))
            _removalController.add(entry.key);
        }
      }
    } else {
      for (MapEntry<String, T> entry in _state.entries) {
        if ((_filterInserted || (!_filterInserted
            && !_inserted.containsKey(entry.key)))) {
          if (!_filter(entry.key, entry.value))
            _removalController.add(entry.key);
        }
      }
    }
  }

  void deleteFilter() {
    if (_closed)
      throw Exception("The fireblend stream provider has already been closed.");
    bool Function(String, T) oldFilter = _filter;
    _filter = null;
    _stateController.add(_state);
    if (oldFilter != null) {
      for (MapEntry<String, T> entry in _state.entries) {
        if (!oldFilter(entry.key, entry.value)
            && (_filterInserted || (!_filterInserted
            && !_inserted.containsKey(entry.key))))
          _additionController.add(entry);
      }
    }
  }

  @override
  void _addElement(String source, FireblendStreamElement<T> element) {
    _subscribe(source, element.added.listen((MapEntry<String, T> entry) {
      if (_sources[entry.key] == null) _sources[entry.key] = Set();
      _sources[entry.key].add(source);
      if (!_state.containsKey(entry.key)) {
        _state.addEntries([entry]);
        _stateController.add(_state);
        if (_filter != null) {
          if (_filter(entry.key, entry.value))
            _additionController.add(entry);
        } else _additionController.add(entry);
      }
    }));
    _subscribe(source, element.modified.listen((MapEntry<String, T> entry) {
      if (_state.containsKey(entry.key)) {
        _state.addEntries([entry]);
        _stateController.add(_state);
        if (_filter != null) {
          if (_filter(entry.key, entry.value))
            _modificationController.add(entry);
        } else _modificationController.add(entry);
      }
    }));
    _subscribe(source, element.removed.listen((String key) {
      if (_sources[key] != null) _sources[key].remove(source);
      if (_sources[key]?.isNotEmpty ?? false) return;
      if (_state.containsKey(key) && !_inserted.containsKey(key)) {
        T value = _state[key];
        _state.remove(key);
        _stateController.add(_state);
        if (_filter != null) {
          if (_filter(key, value))
            _removalController.add(key);
        } else _removalController.add(key);
      }
    }));
  }

  @override
  void _deleteElement(String source) {
    Map<String, T> state = _elements[source]?.currentState() ?? Map();
    for (String key in state.keys) {
      if (_sources[key] != null) _sources[key].remove(source);
      if (_sources[key]?.isNotEmpty ?? false) continue;
      if (!_inserted.containsKey(key)) {
        T value = _state[key];
        _state.remove(key);
        _stateController.add(_state);
        if (_filter != null) {
          if (_filter(key, value))
            _removalController.add(key);
        } else _removalController.add(key);
      }
    }
  }

  @override
  Future _clear() async {
    Map<String, T> state = Map.from(_state);
    _state.removeWhere((key, value) => !_inserted.containsKey(key));
    _stateController.add(_state);
    _sources.clear();
    for (String key in state.keys) {
      if (!_inserted.containsKey(key)) {
        if (_filter != null) {
          if (_filter(key, state[key]))
            _removalController.add(key);
        } else _removalController.add(key);
      }
    }
  }

  @override
  Future _close() async {
    _stateController.add(Map());
    for (String key in _state.keys)
      _removalController.add(key);
    _state.clear();
    _inserted.clear();
    List<Future> futures = List();
    futures.add(_stateController.close());
    futures.add(_additionController.close());
    futures.add(_modificationController.close());
    futures.add(_removalController.close());
    await Future.wait(futures);
  }
}

class ValueStreamProvider<T> extends FireblendStreamProvider<T> {
  MapEntry<String, T> _state;
  Set<String> _sources;

  BehaviorSubject<MapEntry<String, T>> _stateController;

  ValueStreamProvider() {
    _state = null;
    _sources = Set();
    _stateController = BehaviorSubject<MapEntry<String, T>>();
    _stateController.add(_state);
  }

  ValueStream<T> get readable => ValueStream._(this);

  Observable<MapEntry<String, T>> get state =>
      _stateController.stream.map((entry) {
        if (entry == null) return null;
        else return MapEntry(entry.key, entry.value);
      });

  MapEntry<String, T> currentState() {
    if (_state == null) return null;
    else return MapEntry(_state.key, _state.value);
  }

  Future<MapEntry<String, T>> once() async {
    List<Future> futures = List();
    for (String key in _elements.keys) {
      FireblendStreamElement element = _elements[key];
      if (element is ValueStreamElement)
        futures.add(element.once());
    }
    Map<String, T> accumulator = Map();
    List<dynamic> results = await Future.wait(futures);
    results = results.expand((e) => e).toList(); // Flatten results array.
    for (int i = 0; i < results.length; i++)
      accumulator = _combine(accumulator, results[i]);
    accumulator = _combine(accumulator, _state);
    if (accumulator.isNotEmpty)
      return accumulator.entries
          .firstWhere((entry) => entry.value != null, orElse: () => null);
    else return null;
  }

  @override
  void _addElement(String source, FireblendStreamElement<T> element) {
    _subscribe(source, element.state.listen((dynamic state) {
      if (state == null || (state is Map && state.isEmpty)) {
        _sources.remove(source);
        if (_sources.isEmpty) _state = null;
      } else if (state is Map) {
        _sources.add(source);
        _state = state.entries.first;
      } else if (state is MapEntry) {
        _sources.add(source);
        _state = state;
      }
      _stateController.add(_state);
    }));
  }

  @override
  void _deleteElement(String source) {}

  @override
  Future _clear() async {
    _state = null;
    _stateController.add(_state);
  }

  @override
  Future _close() async => await _stateController.close();
}

abstract class FireblendStreamProvider<T> {
  Map<String, FireblendStreamElement<T>> _elements;
  Map<String, StreamSubscription> _subscriptions;
  Map<String, Set<String>> _mapping;

  bool _closed;

  FireblendStreamProvider() {
    _elements = Map();
    _subscriptions = Map();
    _mapping = Map();
    _closed = false;
  }

  bool get closed => _closed;

  void readyListeners() {
    if (_closed)
      throw Exception("The fireblend stream provider has already been closed.");
    for (String key in _elements.keys) _elements[key].readyListeners();
  }

  void addElement(FireblendStreamElement<T> element, {String key}) {
    if (_closed)
      throw Exception("The fireblend stream provider has already been closed.");
    if (key == null) key = Random().nextDouble().toString();
    if (_elements[key] != null) deleteElement(key);
    _elements[key] = element;
    _addElement(key, element);
  }

  void _addElement(String source, FireblendStreamElement<T> component);

  void deleteElement(String key) {
    if (_closed)
      throw Exception("The fireblend stream provider has already been closed.");
    _deleteElement(key);
    _elements[key]?.close();
    _elements?.remove(key);
    _cancel(key);
  }

  void _deleteElement(String source);

  bool containsElement(String key) =>
      _elements.containsKey(key);

  Future _cancel(String source) async {
    if (_mapping.containsKey(source)) {
      List<Future> futures = List();
      for (String key in _mapping[source])
        futures.add(_subscriptions[key]?.cancel() ?? Future.value());
      _mapping.remove(source);
      await Future.wait(futures);
    }
  }

  Future _subscribe(String source, StreamSubscription subscription) async {
    if (_closed) {
      await subscription.cancel();
    } else {
      String key = Random().nextDouble().toString();
      await _subscriptions[key]?.cancel();
      _subscriptions[key] = subscription;
      if (_mapping[source] == null) _mapping[source] = Set();
      _mapping[source].add(key);
    }
  }

  Map<String, T> _combine(dynamic left, dynamic right) {
    if (left is Map && right == null)
      return Map<String, T>.from(left);
    else if (left == null && right is Map)
      return Map<String, T>.from(right);
    else if (left is Map && right is Map)
      return Map<String, T>.from(left)..addEntries(right?.entries ?? Map());
    else if (left is Map && right is MapEntry)
      return Map<String, T>.from(left)..addEntries([right]);
    else if (left is MapEntry && right is Map)
      return Map<String, T>.from(right)..addEntries([left]);
    else if (left is MapEntry && right == null)
      return Map<String, T>.fromEntries([left]);
    else if (left == null && right is MapEntry)
      return Map<String, T>.fromEntries([right]);
    else if (left is MapEntry && right is MapEntry)
      return Map<String, T>.fromEntries([left, right]);
    else return Map<String, T>();
  }

  Future clear() async {
    if (_closed)
      throw Exception("The fireblend stream provider has already been closed.");
    List<Future> futures = List();
    for (String key in _subscriptions.keys)
      futures.add(_subscriptions[key].cancel());
    _subscriptions.clear();
    for (String key in _elements.keys)
      futures.add(_elements[key].close());
    _elements.clear();
    _mapping.clear();
    await Future.wait(futures);
    await _clear();
  }

  Future _clear();

  // In Dart 1.x, async functions immediately suspended execution.
  // In Dart 2, instead of immediately suspending, async functions
  // execute synchronously until the first await or return.
  Future close() async {
    if (!_closed) {
      Future future = clear();
      _closed = true;
      await future;
      await _close();
    }
  }

  Future _close();
}
