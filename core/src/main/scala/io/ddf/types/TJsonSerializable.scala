/*
 *  Copyright (C) 2013 Adatao, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.ddf.types

import java.lang.reflect.Type
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonDeserializationContext
import com.google.gson.JsonDeserializer
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonParseException
import com.google.gson.JsonSerializationContext
import com.google.gson.JsonSerializer
import com.google.gson.JsonPrimitive
import com.google.gson.JsonNull
import com.google.gson.JsonArray

/**
 * Every [[TJsonSerializable]] can provide its own (override) fromJson() and toJson().
 * It also automatically has a val "class" containing its class name.
 *
 * You should only mix in this trait if you really need to provide your own fromJson() and toJson(),
 * or if you need to have the class info embedded in the "clazz" field for clients.
 * Otherwise the standard serdes would be sufficient.
 */
trait TJsonSerializable extends Serializable {
	val clazz = this.getClass.getName

	/**
	 * Override toJson() to implement the class's own preferred deserialization
	 */
	def fromJson(jsonString: String): TJsonSerializable = TJsonSerializable.fromJson(jsonString, this.getClass)

	/**
	 * Override toJson() to implement the class's own preferred serialization
	 */
	def toJson: String = TJsonSerializable.toJson(this)
}

/**
 * Supply the default deserializer for this type
 */
object TJsonSerializable {
	/**
	 * A common Gson object for basic serdes
	 */
	private[types] lazy val basicGson: Gson = new GsonBuilder().serializeSpecialFloatingPointValues().create()

	private lazy val gsonNoTJsonSerializable: Gson = getGsonNoTJsonSerializable

	/**
	 * Provides a fromJson() that recognizes all specially supported types, *except* for TJsonSerializable itself (to avoid infinite loops)
	 */
	def fromJson[T](json: String, classOfT: Class[T]): T = gsonNoTJsonSerializable.fromJson(json, classOfT)

	/**
	 * Provides a toJson() that recognizes all specially supported types, *except* for TJsonSerializable itself (to avoid infinite loops)
	 */
	def toJson(obj: Any): String = gsonNoTJsonSerializable.toJson(obj)

	/**
	 * Returns a new Gson with both the standard serializer and deserializer registered for all types for which
	 * we have special serdes support, including TJsonSerializable itself.
	 */
	def getGsonBuilder: GsonBuilder = TJsonSerializable
		.getGsonBuilderNoTJsonSerializable
		.registerTypeHierarchyAdapter(classOf[TJsonSerializable], new TJsonSerializableSerializer)
		.registerTypeHierarchyAdapter(classOf[TJsonSerializable], new TJsonSerializableDeserializer)

	/**
	 * Returns a new Gson with both the standard serializer and deserializer registered for all types for which
	 * we have special serdes support, excluding TJsonSerializable itself (to avoid infinite loops)
	 */
	private def getGsonBuilderNoTJsonSerializable: GsonBuilder = TJsonSerializable.registerSerializers(TJsonSerializable.registerDeserializers(new GsonBuilder))

	/**
	 * Returns a new Gson with both the standard serializer and deserializer registered
	 */
	def getGson: Gson = TJsonSerializable.getGsonBuilder.create

	private def getGsonNoTJsonSerializable = TJsonSerializable.getGsonBuilderNoTJsonSerializable.create

	/**
	 * Registers the standard serializer with the given gsonBuilder
	 *
	 * @param gsonBuilder - GsonBuilder with which to register
	 */
	private def registerSerializers(gsonBuilder: GsonBuilder): GsonBuilder = gsonBuilder
		.registerTypeHierarchyAdapter(classOf[Exception], new SpecialSerDes.ExceptionSerializer)
		.registerTypeHierarchyAdapter(classOf[java.lang.Float], new SpecialSerDes.FloatSerializer)
		.registerTypeHierarchyAdapter(classOf[java.lang.Double], new SpecialSerDes.DoubleSerializer)
		.registerTypeHierarchyAdapter(classOf[Product], new SpecialSerDes.ProductSerializer)

	/**
	 * Registers the standard deserializer with the given gsonBuilder
	 *
	 * @param gsonBuilder - GsonBuilder with which to register
	 */
	private def registerDeserializers(gsonBuilder: GsonBuilder): GsonBuilder = gsonBuilder
		.registerTypeHierarchyAdapter(classOf[Exception], new SpecialSerDes.ExceptionDeserializer)
		.registerTypeHierarchyAdapter(classOf[java.lang.Float], new SpecialSerDes.FloatDeserializer)
		.registerTypeHierarchyAdapter(classOf[java.lang.Double], new SpecialSerDes.DoubleDeserializer)
		.registerTypeHierarchyAdapter(classOf[Product], new SpecialSerDes.ProductDeserializer)
}

/**
 * Standard deserializer (fromJson) for [[TJsonSerializable]]. We get the class name from the field "clazz",
 * and force deserialization to that type using our own standard Gson.
 */
private class TJsonSerializableDeserializer extends JsonDeserializer[TJsonSerializable] {

	def deserialize(jElem: JsonElement, typeOfT: Type, context: JsonDeserializationContext): TJsonSerializable = {
		// First determine class name from "clazz" field or typeOfT, in that order
		val className: String = jElem match {
			case jObj: JsonObject ⇒ Option(jObj.get("clazz")) match {
				case Some(jEle) ⇒ jEle.getAsString()
				case None ⇒ typeOfT.toString.replace("class ", "")
			}
			case _ ⇒ typeOfT.toString.replace("class ", "")
		}

		// Now use that class name to deserialize
		Option(Class.forName(className)) match {
			case Some(validClass) ⇒ Option(validClass.getConstructor()) match {
				case Some(validConstructor) ⇒ validConstructor.newInstance() match {
					case validObject: TJsonSerializable ⇒ validObject.fromJson(jElem.toString) // defer to the class's own fromJson
					case _ ⇒ throw new JsonParseException("Failed to instantiate an object of type %s".format(className))
				}
				case None ⇒ throw new JsonParseException("Cannot find no-argument constructor for type %s. Please implement one for deserialization.".format(className))
			}
			case None ⇒ throw new JsonParseException("Failed to get Class object for type %s".format(className))
		}
	}
}

/**
 * Standard serializer (toJson) for [[TJsonSerializable]]. We simply do standard serialization using our own Gson object.
 */
private class TJsonSerializableSerializer extends JsonSerializer[TJsonSerializable] {

	def serialize(obj: TJsonSerializable, typeOfT: Type, context: JsonSerializationContext): JsonElement = {
		// Defer to the object's own toJson(), then take that jsonString and convert it into the required JsonElement
		obj.toJson match {
			case jsonString: String ⇒ TJsonSerializable.basicGson.fromJson(jsonString, classOf[JsonElement])
			case _ ⇒ null
		}
	}
}

private object SpecialSerDes {
	private lazy val standardGson = new GsonBuilder().create // used when we need to serdes without going through the registered type hierarchy

	class ExceptionSerializer extends JsonSerializer[Exception] {
		def serialize(e: Exception, typeOfT: Type, context: JsonSerializationContext): JsonElement = {
			new JsonPrimitive(e.getStackTraceString)
		}
	}

	class ExceptionDeserializer extends JsonDeserializer[Exception] {
		def deserialize(jElem: JsonElement, typeOfT: Type, context: JsonDeserializationContext): Exception = {
			new Exception(jElem.getAsString)
		}
	}

	class FloatSerializer extends JsonSerializer[Float] {
		def serialize(obj: Float, typeOfT: Type, context: JsonSerializationContext): JsonElement = {
			if (obj.isNaN) JsonNull.INSTANCE // per RClient preference
			else obj match {
				case Float.PositiveInfinity ⇒ new JsonPrimitive("Infinity")
				case Float.NegativeInfinity ⇒ new JsonPrimitive("-Infinity")
				case _ ⇒ new JsonPrimitive(obj)
			}
		}
	}

	class FloatDeserializer extends JsonDeserializer[Float] {
		def deserialize(jElem: JsonElement, typeOfT: Type, context: JsonDeserializationContext): Float = {
			jElem match {
				case x: JsonNull ⇒ Float.NaN
				case _ ⇒ jElem.toString match {
					case "\"NaN\"" ⇒ Float.NaN
					case "\"NA\"" ⇒ Float.NaN
					case "\"Infinity\"" ⇒ Float.PositiveInfinity
					case "\"-Infinity\"" ⇒ Float.NegativeInfinity
					case str: String ⇒ java.lang.Float.parseFloat(str)
					case _ ⇒ throw new JsonParseException("Cannot deserialize %s as Float".format(jElem))
				}
			}
		}
	}

	class DoubleSerializer extends JsonSerializer[Double] {
		def serialize(obj: Double, typeOfT: Type, context: JsonSerializationContext): JsonElement = {
			if (obj.isNaN) JsonNull.INSTANCE // per RClient preference
			else obj match {
				case Double.PositiveInfinity ⇒ new JsonPrimitive("Infinity")
				case Double.NegativeInfinity ⇒ new JsonPrimitive("-Infinity")
				case _ ⇒ new JsonPrimitive(obj)
			}
		}
	}

	class DoubleDeserializer extends JsonDeserializer[Double] {
		def deserialize(jElem: JsonElement, typeOfT: Type, context: JsonDeserializationContext): Double = {
			jElem match {
				case x: JsonNull ⇒ Double.NaN
				case _ ⇒ jElem.getAsString match {
					case "\"NaN\"" ⇒ Double.NaN
					case "\"NA\"" ⇒ Double.NaN
					case "\"Infinity\"" ⇒ Double.PositiveInfinity
					case "\"-Infinity\"" ⇒ Double.NegativeInfinity
					case str: String ⇒ java.lang.Double.parseDouble(str)
					case _ ⇒ throw new JsonParseException("Cannot deserialize %s as Double".format(jElem))
				}
			}
		}
	}

	/**
	 * Serializes a TupleX <: Product into {"tuple":[..., ..., ...],"types":[..., ..., ...]}.
	 */
	class ProductSerializer extends JsonSerializer[Product] {
		def serialize(obj: Product, typeOfT: Type, context: JsonSerializationContext): JsonElement = {
			Option(obj) match {
				case Some(validObj) ⇒ if (validObj.getClass.getSimpleName.startsWith("Tuple")) {
					val result = new JsonObject
					val values = new JsonArray
					val types = new JsonArray
					result.add("tuple", values)
					result.add("types", types)
					obj.productIterator.foreach { x ⇒
						{
							values.add(context.serialize(x))
							types.add(context.serialize(if (x == null) null else x.getClass.getName))
						}
					}
					result
				}
				else {
					standardGson.fromJson(standardGson.toJson(obj), classOf[JsonObject])
				}
				case None ⇒ JsonNull.INSTANCE
			}
		}
	}

	/**
	 * Deserializes a string {"tuple":[..., ..., ...],"types":[..., ..., ...]} into a TupleX <: Product.
	 * Supports up to Tuple10 only.
	 */
	class ProductDeserializer extends JsonDeserializer[Product] {
		def deserialize(jElem: JsonElement, typeOfT: Type, context: JsonDeserializationContext): Product = {
			jElem match {
				case jObj: JsonObject ⇒ {
					val jTuple = jObj.get("tuple").getAsJsonArray
					val jTypes = jObj.get("types").getAsJsonArray
					val arity = jTuple.size
					val elements = for (i ← 0 to arity - 1) yield context.deserialize(jTuple.get(i), Class.forName(jTypes.get(i).getAsString)).asInstanceOf[Any]
					arity match {
						case 1 ⇒ new Tuple1(elements(0))
						case 2 ⇒ new Tuple2(elements(0), elements(1))
						case 3 ⇒ new Tuple3(elements(0), elements(1), elements(2))
						case 4 ⇒ new Tuple4(elements(0), elements(1), elements(2), elements(3))
						case 5 ⇒ new Tuple5(elements(0), elements(1), elements(2), elements(3), elements(4))
						case 6 ⇒ new Tuple6(elements(0), elements(1), elements(2), elements(3), elements(4), elements(5))
						case 7 ⇒ new Tuple7(elements(0), elements(1), elements(2), elements(3), elements(4), elements(5), elements(6))
						case 8 ⇒ new Tuple8(elements(0), elements(1), elements(2), elements(3), elements(4), elements(5), elements(6), elements(7))
						case 9 ⇒ new Tuple9(elements(0), elements(1), elements(2), elements(3), elements(4), elements(5), elements(6), elements(7), elements(8))
						case 10 ⇒ new Tuple10(elements(0), elements(1), elements(2), elements(3), elements(4), elements(5), elements(6), elements(7), elements(8), elements(9))
						case _ ⇒ standardGson.fromJson(jElem.toString, typeOfT)
					}
				}
				case _ ⇒ standardGson.fromJson(jElem.toString, typeOfT)
			}
		}
	}
}
