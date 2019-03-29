package dev.komu.ahwen.utils

import java.lang.reflect.Proxy

inline fun <reified T> unimplemented(): T =
    Proxy.newProxyInstance(T::class.java.classLoader, arrayOf(T::class.java)) { obj, method, params ->
        if (method.declaringClass == Object::class.java) {
            when (method.name) {
                "equals" -> obj === params[0]
                "hashCode" -> System.identityHashCode(obj)
                "toString" -> "Proxy/${T::class.java.name}#${System.identityHashCode(obj)}"
                else -> error("unknown object method: ${method.name}")
            }
        } else {
            throw UnsupportedOperationException("${method.name} is unimplemented")
        }
    } as T
