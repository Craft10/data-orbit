**Data Orbit**
================

Un sistema de base de datos simple y fácil de usar.

**Instalación**
---------------

Para instalar `data-orbit`, ejecuta el siguiente comando en tu terminal:

```bash
npm install data-orbit
```

**Uso**
-----

Para usar `data-orbit`, crea una instancia de la clase `DataOrbit` y configúrala con tus opciones:

```javascript
const DataOrbit = require('data-orbit');

const config = {
  file: './database.json',
  encryptionKey: 'tu-clave-de-criptación',
  tables: {
    usuarios: {
      id: 'Text',
      nombre: 'Text',
      edad: 'Number',
    },
  },
};

const db = new DataOrbit(config);
```

**Métodos**
------------

* `insert(table, data)`: Inserta un nuevo registro en la tabla especificada.
* `getRow(table, id)`: Obtiene un registro de la tabla especificada por su ID.
* `update(table, id, data)`: Actualiza un registro de la tabla especificada.
* `delete(table, id)`: Elimina un registro de la tabla especificada.

**Ejemplos**
-------------

```javascript
// Insertar un nuevo usuario
db.insert('usuarios', {
  id: 1,
  nombre: 'Juan',
  edad: 25,
});

// Obtener un usuario por su ID
const user = db.getRow('usuarios', 1);
console.log(user);

// Actualizar un usuario
db.update('usuarios', 1, {
  nombre: 'Juanito',
});

// Eliminar un usuario
db.delete('usuarios', 1);
```

**Licencia**
------------

`data-orbit` está licenciado bajo la licencia MIT.

**Contribuciones**
-----------------

Si deseas contribuir a `data-orbit`, por favor, crea un fork del repositorio y envía una solicitud de extracción.

**Repositorio**
---------------

El repositorio de `data-orbit` se encuentra en [https://github.com/tu-usuario/data-orbit](https://github.com/tu-usuario/data-orbit).