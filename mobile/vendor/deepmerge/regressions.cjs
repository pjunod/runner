const test = require('node:test');
const assert = require('node:assert/strict');
const merge = require('./index.cjs');
const poison = () => JSON.parse('{"__proto__":{"injected":true}}');
function safe(value) { assert.equal(Object.getPrototypeOf(value), Object.prototype); assert.equal(value.injected, undefined); assert.equal({}.injected, undefined); }
test('null-prototype target never invokes the prototype setter', () => { const result = merge(Object.create(null), poison()); safe(result); assert.equal(Object.hasOwn(result, '__proto__'), true); assert.equal(result.__proto__.injected, true); });
test('own poison key in target remains data', () => { const result = merge(poison(), {}); safe(result); assert.equal(Object.hasOwn(result, '__proto__'), true); });
test('nested poison does not alter result prototypes', () => { const result = merge({nested:Object.create(null)}, {nested:poison()}); safe(result.nested); });
test('constructor and inherited properties remain guarded', () => { const result = merge({}, JSON.parse('{"constructor":{"prototype":{"injected":true}},"__proto__":{"injected":true}}')); safe(result); assert.equal(Object.hasOwn(result,'constructor'), false); });
test('merge API preserves symbols, cloning, arrays and all', () => { const symbol=Symbol('key'); const value={a:{b:1}, list:[{x:1}], [symbol]:3}; const result=merge(value,{a:{c:2},list:[2]}); assert.deepEqual(result.a,{b:1,c:2}); assert.deepEqual(result.list,[{x:1},2]); assert.notEqual(result.list[0],value.list[0]); assert.equal(result[symbol],3); assert.equal(merge({},value,{clone:false}).a,value.a); assert.deepEqual(merge.all([{a:1},{b:2}]),{a:1,b:2}); assert.throws(()=>merge.all({})); });
test('custom merge and array merge options remain supported', () => { assert.deepEqual(merge({a:{x:1}},{a:{y:2}},{customMerge:key=>key==='a'?()=>({custom:true}):undefined}),{a:{custom:true}}); assert.deepEqual(merge([1],[2],{arrayMerge:(_,source)=>source}),[2]); });
test('special values preserve identity', () => { for (const value of [new Date(), /abc/, {$$typeof:Symbol.for('react.element')}]) assert.equal(merge({}, {value}).value,value); });
