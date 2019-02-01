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

class CollectionStreamProvider<T> extends FireblendStreamProvider<T>{
  Map<String, T> _state;
  Map<String, T> _filtered;
  bool Function(String, T) _filter;
  Map<String, Set<String>> _sources;
  BehaviorSubject<Map<String, T>> _stateController;
  StreamController<MapEntry<String, T>> _additionController;
  StreamController<MapEntry<String, T>> _modificationController;
  StreamController<String> _removalController;

  CollectionStreamProvider() {
    _state = Map();
    _filtered = Map();
    _sources = Map();
    _stateController = BehaviorSubject<Map<String, T>>();
    _additionController = StreamController<MapEntry<String, T>>.broadcast();
    _modificationController = StreamController<MapEntry<String, T>>.broadcast();
    _removalController = StreamController<String>.broadcast();
    _stateController.add(_filtered);
  }

  CollectionStream<T> get readable => CollectionStream._(this);

  Observable<Map<String, T>> get state => _stateController.stream;

  Stream<MapEntry<String, T>> get addition => _additionController.stream;

  Stream<MapEntry<String, T>> get modification => _modificationController.stream;

  Stream<String> get removal => _removalController.stream;

  Map<String, T> currentState() => _filtered;

  void insert(MapEntry<String, T> entry) {
    if (_closed)
      throw Exception("The fireblend stream provider has already been closed.");
    bool contained = _state.containsKey(entry.key);
    _state.addEntries([entry]);
    if (_filter != null) {
      if (_filter(entry.key, entry.value)) {
        _filtered.addEntries([entry]);
        _stateController.add(_filtered);
        if (contained) _modificationController.add(entry);
        else _additionController.add(entry);
      }
    } else {
      _filtered.addEntries([entry]);
      _stateController.add(_filtered);
      if (contained) _modificationController.add(entry);
      else _additionController.add(entry);
    }
  }

  void remove(String key) {
    if (_closed)
      throw Exception("The fireblend stream provider has already been closed.");
    if (_state.containsKey(key)
        && (!_sources.containsKey(key)
            || (_sources[key]?.isEmpty ?? true))) {
      _state.remove(key);
      if (_filtered.containsKey(key)) {
        _filtered.remove(key);
        _stateController.add(_filtered);
        _removalController.add(key);
      }
    }
  }

  void addFilter(bool Function(String, T) filter) {
    if (_closed)
      throw Exception("The fireblend stream provider has already been closed.");
    _filter = filter;
    for (MapEntry<String, T> entry in _state.entries) {
      if (filter(entry.key, entry.value)) {
        if (!_filtered.containsKey(entry.key)) {
          _filtered.addEntries([entry]);
          _stateController.add(_filtered);
          _additionController.add(entry);
        }
      } else {
        if (_filtered.containsKey(entry.key)) {
          _filtered.remove(entry.key);
          _stateController.add(_filtered);
          _removalController.add(entry.key);
        }
      }
    }
  }

  void deleteFilter() {
    if (_closed)
      throw Exception("The fireblend stream provider has already been closed.");
    _filter = null;
    for (MapEntry<String, T> entry in _state.entries) {
      if (!_filtered.containsKey(entry.key)) {
        _filtered.addEntries([entry]);
        _stateController.add(_filtered);
        _additionController.add(entry);
      }
    }
  }

  @override
  void _addElement(String source, FireblendStreamElement<T> element) {
    _subscribe(source, element.added.listen((MapEntry<String, T> entry) {
      if (_sources[entry.key] == null)
        _sources[entry.key] = Set();
      _sources[entry.key].add(source);
      if (!_state.containsKey(entry.key)) {
        _state.addEntries([entry]);
        if (_filter != null) {
          if (_filter(entry.key, entry.value)) {
            _filtered.addEntries([entry]);
            _stateController.add(_filtered);
            _additionController.add(entry);
          }
        } else {
          _filtered.addEntries([entry]);
          _stateController.add(_filtered);
          _additionController.add(entry);
        }
      }
    }));
    _subscribe(source, element.modified.listen((MapEntry<String, T> entry) {
      if (_state.containsKey(entry.key)) {
        _state.addEntries([entry]);
        if (_filtered.containsKey(entry.key)) {
          _filtered.addEntries([entry]);
          _stateController.add(_filtered);
          _modificationController.add(entry);
        }
      }
    }));
    _subscribe(source, element.removed.listen((String key) {
      if (_sources[key] != null) _sources[key].remove(source);
      if (_sources[key]?.isNotEmpty ?? false) return;
      if (_state.containsKey(key)) {
        _state.remove(key);
        if (_filtered.containsKey(key)) {
          _filtered.remove(key);
          _stateController.add(_filtered);
          _removalController.add(key);        }
      }
    }));
  }

  @override
  void _deleteElement(String source) {
    Map<String, T> state = _elements[source].currentState();
    for (String key in state.keys) {
      if (_sources[key] != null) _sources[key].remove(source);
      if (_sources[key]?.isNotEmpty ?? false) continue;
      _state.remove(key);
      if (_filtered.containsKey(key)) {
        _filtered.remove(key);
        _stateController.add(_filtered);
        _removalController.add(key);
      }
    }
  }

  @override
  Future _clear() async {
    Map<String, Set<String>> sources = Map.from(_sources);
    Map<String, T> filtered = Map.from(_filtered);
    _state.removeWhere((key, value) => sources[key]?.isNotEmpty ?? false);
    _filtered.removeWhere((key, value) => sources[key]?.isNotEmpty ?? false);
    _stateController.add(_filtered);
    _sources.clear();
    for (String key in sources.keys)
      if ((sources[key]?.isNotEmpty ?? false)
          && filtered.containsKey(key))
        _removalController.add(key);
  }

  @override
  Future _close() async {
    List<Future> futures = List();
    futures.add(_stateController.close());
    futures.add(_additionController.close());
    futures.add(_modificationController.close());
    futures.add(_removalController.close());
    await Future.wait(futures);
  }
}

class ValueStreamProvider<T> extends FireblendStreamProvider<T>{
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

  Observable<MapEntry<String, T>> get state => _stateController.stream;

  MapEntry<String, T> currentState() => _state;

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
    if (accumulator.isNotEmpty) return accumulator.entries
        .firstWhere((entry) => entry.value != null, orElse: () => null);
    else return null;
  }

  @override
  void _addElement(String source, FireblendStreamElement<T> element) {
    _subscribe(source, element.state.listen((dynamic state) {
      if (state == null || (state is Map && state.isEmpty)) {
        _sources.remove(source);
        if (_sources.isEmpty)
          _state = null;
      } else if (state is Map) {
        _sources.add(source);
        _state = state.entries.first;
      } else if (state is MapEntry) {
        _sources.add(source);
        _state = state;
      } _stateController.add(_state);
    }));
  }

  @override
  void _deleteElement(String source) {}

  @override
  Future _clear() async => _stateController.add(null);

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
    for (String key in _elements.keys)
      _elements[key].readyListeners();
  }

  void addElement(FireblendStreamElement<T> element, {String key}) {
    if (_closed)
      throw Exception("The fireblend stream provider has already been closed.");
    if (key == null)
      key = Random().nextDouble().toString();
    _elements[key] = element;
    _addElement(key, element);
  }

  void _addElement(String source, FireblendStreamElement<T> component);

  void deleteElement(String key) {
    if (_closed)
      throw Exception("The fireblend stream provider has already been closed.");
    _deleteElement(key);
    _elements[key].close();
    _elements.remove(key);
    _cancel(key);
  }

  void _deleteElement(String source);

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
    if (_closed){
      await subscription.cancel();
    } else {
      String key = Random().nextDouble().toString();
      await _subscriptions[key]?.cancel();
      _subscriptions[key] = subscription;
      if (_mapping[source] == null)
        _mapping[source] = Set();
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
    else  if (left is MapEntry && right is Map)
      return Map<String, T>.from(right)..addEntries([left]);
    else  if (left is MapEntry && right == null)
      return Map<String, T>.fromEntries([left]);
    else if (left == null && right is MapEntry)
      return Map<String, T>.fromEntries([right]);
    else  if (left is MapEntry && right is MapEntry)
      return Map<String, T>.fromEntries([left, right]);
    else return Map<String, T>();
  }

  Future clear() async {
    if (_closed)
      throw Exception("The fireblend stream provider has already been closed.");
    await _clearAux();
  }

  Future _clearAux() async {
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

  Future close() async {
    if (!_closed) {
      _closed = true;
      await _clearAux();
      await _close();
    }
  }

  Future _close();
}