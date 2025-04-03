const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

/**
 * Tipos de datos soportados
 */
const DataType = {
  TEXT: 'Text',
  NUMBER: 'Number',
  BOOLEAN: 'Boolean',
  DATE: 'Date',
  OBJECT: 'Object',
  ARRAY: 'Array',
  PATH: 'Path'
};

/**
 * DataOrbit - Sistema de base de datos NoSQL basado en JSON
 */
class DataOrbit {
  /**
   * Constructor de la clase DataOrbit
   * @param {Object} config - Configuración de la base de datos
   * @param {string} config.file - Ruta del archivo de la base de datos
   * @param {string} config.encryptionKey - Clave de cifrado
   * @param {Object} config.tables - Definición de tablas
   * @param {Array} config.backups - Configuración de copias de seguridad
   */
  constructor(config) {
    this.config = this._validateConfig(config);
    this.data = {};
    this.primaryKeyMap = {};
    this.uniqueKeyMap = {};
    this.indexes = {};
    
    // Crear directorio si no existe
    const dir = path.dirname(this.config.file);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    
    // Cargar la base de datos
    this.loadDatabase();
    
    // Iniciar el servicio de backup si está configurado
    if (this.config.backups && this.config.backups.length > 0) {
      this.startBackupService();
    }
    
    // Inicializar índices
    this._initializeIndexes();
  }
  
  /**
   * Valida la configuración inicial
   * @private
   */
  _validateConfig(config) {
    if (!config) throw new Error('La configuración es obligatoria');
    if (!config.file) throw new Error('Debe especificar el archivo de la base de datos');
    if (!config.encryptionKey) throw new Error('Debe especificar una clave de cifrado');
    
    // Establecer valores por defecto
    return {
      file: config.file,
      encryptionKey: config.encryptionKey,
      tables: config.tables || {},
      backups: config.backups || [{ interval: 1 }], // Por defecto, backup diario
      connectionTimeout: config.connectionTimeout || 5000
    };
  }
  
  /**
   * Inicializa los índices de las tablas
   * @private
   */
  _initializeIndexes() {
    for (const tableName in this.data) {
      if (!this.indexes[tableName]) {
        this.indexes[tableName] = {};
      }
      
      const tableConfig = this.config.tables[tableName];
      if (!tableConfig) continue;
      
      // Crear índice para primaryKey
      const primaryKey = tableConfig.primaryKey || 'id';
      this._createIndex(tableName, primaryKey);
      
      // Crear índices para campos únicos
      const uniqueFields = tableConfig.unique || [];
      for (const field of uniqueFields) {
        this._createIndex(tableName, field);
      }
      
      // Calcular el próximo valor de la clave primaria
      this._calculateNextPrimaryKey(tableName, primaryKey);
    }
  }
  
  /**
   * Crea un índice para un campo específico
   * @private
   */
  _createIndex(tableName, field) {
    if (!this.indexes[tableName]) {
      this.indexes[tableName] = {};
    }
    
    this.indexes[tableName][field] = {};
    
    // Poblar el índice con datos existentes
    if (this.data[tableName]) {
      this.data[tableName].forEach((row, index) => {
        if (row[field] !== undefined) {
          this.indexes[tableName][field][row[field]] = index;
        }
      });
    }
  }
  
  /**
   * Calcula el siguiente valor para la clave primaria
   * @private
   */
  _calculateNextPrimaryKey(tableName, primaryKey) {
    if (!this.data[tableName] || this.data[tableName].length === 0) {
      this.primaryKeyMap[tableName] = 1;
      return;
    }
    
    // Encontrar el valor máximo actual
    let maxValue = 0;
    this.data[tableName].forEach(row => {
      if (typeof row[primaryKey] === 'number' && row[primaryKey] > maxValue) {
        maxValue = row[primaryKey];
      }
    });
    
    this.primaryKeyMap[tableName] = maxValue + 1;
  }

  /**
   * Carga la base de datos desde el archivo
   */
  loadDatabase() {
    try {
      if (!fs.existsSync(this.config.file)) {
        // Si el archivo no existe, crear uno nuevo
        fs.writeFileSync(this.config.file, this.encryptData('{}', this.config.encryptionKey));
        this.data = {};
        return;
      }

      const rawData = fs.readFileSync(this.config.file, 'utf8');
      
      if (rawData.trim() !== '') {
        const decryptedData = this.decryptData(rawData, this.config.encryptionKey);
        this.data = JSON.parse(decryptedData);
        
        // Inicializar mapas de claves únicas
        this._initializeUniqueKeyMaps();
      } else {
        this.data = {};
      }
    } catch (error) {
      console.error('Error al cargar la base de datos:', error);
      throw new Error(`No se pudo cargar la base de datos: ${error.message}`);
    }
  }
  
  /**
   * Inicializa los mapas de claves únicas
   * @private
   */
  _initializeUniqueKeyMaps() {
    this.uniqueKeyMap = {};
    
    for (const tableName in this.data) {
      if (!this.uniqueKeyMap[tableName]) {
        this.uniqueKeyMap[tableName] = {};
      }
      
      const tableConfig = this.config.tables[tableName];
      if (!tableConfig) continue;
      
      const uniqueConstraints = tableConfig.unique || [];
      
      // Inicializar Sets para cada restricción única
      for (const uniqueKey of uniqueConstraints) {
        this.uniqueKeyMap[tableName][uniqueKey] = new Set();
      }
      
      // Poblar los Sets con los valores existentes
      if (this.data[tableName]) {
        for (const row of this.data[tableName]) {
          for (const uniqueKey of uniqueConstraints) {
            if (row[uniqueKey] !== undefined) {
              this.uniqueKeyMap[tableName][uniqueKey].add(row[uniqueKey]);
            }
          }
        }
      }
    }
  }

  /**
   * Guarda la base de datos en el archivo
   */
  saveDatabase() {
    try {
      const encryptedData = this.encryptData(JSON.stringify(this.data, null, 2), this.config.encryptionKey);
      fs.writeFileSync(this.config.file, encryptedData);
      return true;
    } catch (error) {
      console.error('Error al guardar la base de datos:', error);
      throw new Error(`No se pudo guardar la base de datos: ${error.message}`);
    }
  }

  /**
   * Cifra los datos con la clave proporcionada
   * @param {string} data - Datos a cifrar
   * @param {string} encryptionKey - Clave de cifrado
   * @returns {string} - Datos cifrados
   */
  encryptData(data, encryptionKey) {
    // Usar un algoritmo de cifrado más seguro
    const key = crypto.createHash('sha256').update(encryptionKey).digest('base64').substr(0, 32);
    const iv = crypto.randomBytes(16);
    const cipher = crypto.createCipheriv('aes-256-cbc', Buffer.from(key), iv);
    
    let encrypted = cipher.update(data, 'utf8', 'hex');
    encrypted += cipher.final('hex');
    
    // Devolver IV + datos cifrados
    return iv.toString('hex') + ':' + encrypted;
  }

  /**
   * Descifra los datos con la clave proporcionada
   * @param {string} encryptedData - Datos cifrados
   * @param {string} encryptionKey - Clave de cifrado
   * @returns {string} - Datos descifrados
   */
  decryptData(encryptedData, encryptionKey) {
    const key = crypto.createHash('sha256').update(encryptionKey).digest('base64').substr(0, 32);
    
    // Separar IV y datos cifrados
    const parts = encryptedData.split(':');
    if (parts.length !== 2) {
      // Compatibilidad con versiones anteriores (método XOR)
      return this._legacyDecrypt(encryptedData, encryptionKey);
    }
    
    const iv = Buffer.from(parts[0], 'hex');
    const encrypted = parts[1];
    
    const decipher = crypto.createDecipheriv('aes-256-cbc', Buffer.from(key), iv);
    let decrypted = decipher.update(encrypted, 'hex', 'utf8');
    decrypted += decipher.final('utf8');
    
    return decrypted;
  }
  
  /**
   * Método de descifrado antiguo para compatibilidad
   * @private
   */
  _legacyDecrypt(data, encryptionKey) {
    let decrypted = '';
    for (let i = 0; i < data.length; i++) {
      decrypted += String.fromCharCode(data.charCodeAt(i) ^ encryptionKey.charCodeAt(i % encryptionKey.length));
    }
    return decrypted;
  }

  /**
   * Valida los datos contra el esquema definido
   * @private
   */
  _validateDataAgainstSchema(tableName, data) {
    const schema = this.config.tables[tableName]?.schema;
    if (!schema) return true; // Sin esquema, no hay validación
    
    for (const field in schema) {
      const fieldType = schema[field].type;
      const isRequired = schema[field].required === true;
      
      // Verificar campos obligatorios
      if (isRequired && (data[field] === undefined || data[field] === null)) {
        throw new Error(`El campo '${field}' es obligatorio en la tabla '${tableName}'`);
      }
      
      // Si el campo está presente, validar su tipo
      if (data[field] !== undefined && data[field] !== null) {
        if (!this._validateFieldType(data[field], fieldType)) {
          throw new Error(`El campo '${field}' debe ser de tipo ${fieldType}`);
        }
      }
    }
    
    return true;
  }
  
  /**
   * Valida el tipo de un campo
   * @private
   */
  _validateFieldType(value, type) {
    switch (type) {
      case DataType.TEXT:
        return typeof value === 'string';
      case DataType.NUMBER:
        return typeof value === 'number';
      case DataType.BOOLEAN:
        return typeof value === 'boolean';
      case DataType.DATE:
        return value instanceof Date || !isNaN(Date.parse(value));
      case DataType.OBJECT:
        return typeof value === 'object' && !Array.isArray(value) && value !== null;
      case DataType.ARRAY:
        return Array.isArray(value);
      case DataType.PATH:
        return typeof value === 'string' && fs.existsSync(value);
      default:
        return true; // Tipo desconocido, no validar
    }
  }

  /**
   * Inserta un documento en una tabla
   * @param {string} tableName - Nombre de la tabla
   * @param {Object} data - Datos a insertar
   * @returns {Object} - Documento insertado
   */
  insert(tableName, data) {
    // Verificar si la tabla existe, si no, crearla
    if (!this.data[tableName]) {
      this.data[tableName] = [];
      this.indexes[tableName] = {};
    }
    
    // Obtener configuración de la tabla
    const tableConfig = this.config.tables[tableName] || { primaryKey: 'id' };
    const primaryKey = tableConfig.primaryKey || 'id';
    
    // Validar datos contra el esquema
    this._validateDataAgainstSchema(tableName, data);
    
    // Clonar para evitar modificar el original
    const newDoc = { ...data };
    
    // Asignar clave primaria si no tiene
    if (newDoc[primaryKey] === undefined) {
      newDoc[primaryKey] = this.getNextPrimaryKey(tableName);
    }
    
    // Verificar restricciones de unicidad
    if (!this._checkUniqueConstraints(tableName, newDoc)) {
      throw new Error('Violación de restricción de unicidad');
    }
    
    // Añadir metadatos
    const timestamp = new Date().toISOString();
    newDoc._createdAt = timestamp;
    newDoc._updatedAt = timestamp;
    
    // Insertar documento
    this.data[tableName].push(newDoc);
    
    // Actualizar índices y mapas
    const docIndex = this.data[tableName].length - 1;
    this._updateIndexes(tableName, newDoc, docIndex);
    this._updateUniqueKeyMap(tableName, newDoc);
    
    // Guardar cambios
    this.saveDatabase();
    
    return newDoc;
  }

  /**
   * Elimina un documento de una tabla
   * @param {string} tableName - Nombre de la tabla
   * @param {string|number} id - Valor de la clave primaria
   * @returns {boolean} - Éxito de la operación
   */
  delete(tableName, id) {
    if (!this.data[tableName]) {
      return false;
    }
    
    const tableConfig = this.config.tables[tableName] || { primaryKey: 'id' };
    const primaryKey = tableConfig.primaryKey || 'id';
    
    // Buscar el índice del documento
    const index = this.data[tableName].findIndex(item => item[primaryKey] === id);
    if (index === -1) {
      return false;
    }
    
    // Guardar el documento para actualizar los índices
    const deletedDoc = this.data[tableName][index];
    
    // Remover de los índices
    this._removeFromIndexes(tableName, deletedDoc);
    
    // Remover de los mapas de claves únicas
    this._removeFromUniqueKeyMap(tableName, deletedDoc);
    
    // Eliminar el documento
    this.data[tableName].splice(index, 1);
    
    // Guardar cambios
    this.saveDatabase();
    
    return true;
  }

  /**
   * Actualiza un documento en una tabla
   * @param {string} tableName - Nombre de la tabla
   * @param {string|number} id - Valor de la clave primaria
   * @param {Object} newData - Nuevos datos
   * @returns {Object|null} - Documento actualizado o null si no se encontró
   */
  update(tableName, id, newData) {
    if (!this.data[tableName]) {
      return null;
    }
    
    const tableConfig = this.config.tables[tableName] || { primaryKey: 'id' };
    const primaryKey = tableConfig.primaryKey || 'id';
    
    // Buscar el índice del documento
    const index = this.data[tableName].findIndex(item => item[primaryKey] === id);
    if (index === -1) {
      return null;
    }
    
    // Validar datos contra el esquema
    this._validateDataAgainstSchema(tableName, { ...this.data[tableName][index], ...newData });
    
    // Guardar el documento original para actualizar los índices
    const originalDoc = { ...this.data[tableName][index] };
    
    // No permitir cambiar la clave primaria
    if (newData[primaryKey] !== undefined && newData[primaryKey] !== originalDoc[primaryKey]) {
      throw new Error('No se puede modificar la clave primaria');
    }
    
    // Crear el documento actualizado
    const updatedDoc = { 
      ...originalDoc, 
      ...newData,
      _updatedAt: new Date().toISOString() 
    };
    
    // Verificar restricciones de unicidad para los nuevos datos
    if (!this._checkUniqueConstraintsForUpdate(tableName, updatedDoc, originalDoc)) {
      throw new Error('Violación de restricción de unicidad');
    }
    
    // Remover de los índices
    this._removeFromIndexes(tableName, originalDoc);
    
    // Actualizar el documento
    this.data[tableName][index] = updatedDoc;
    
    // Actualizar índices y mapas
    this._updateIndexes(tableName, updatedDoc, index);
    this._updateUniqueKeyMapForUpdate(tableName, updatedDoc, originalDoc);
    
    // Guardar cambios
    this.saveDatabase();
    
    return updatedDoc;
  }

  /**
   * Crea una nueva tabla
   * @param {string} tableName - Nombre de la tabla
   * @param {Object} schema - Esquema de la tabla
   * @returns {boolean} - Éxito de la operación
   */
  createTable(tableName, schema) {
    if (this.data[tableName]) {
      throw new Error(`La tabla '${tableName}' ya existe`);
    }
    
    this.data[tableName] = [];
    
    // Configurar esquema
    if (!this.config.tables[tableName]) {
      this.config.tables[tableName] = {
        primaryKey: 'id',
        schema: schema || {}
      };
    }
    
    // Inicializar índices
    this.indexes[tableName] = {};
    this._createIndex(tableName, this.config.tables[tableName].primaryKey);
    
    // Inicializar mapa de claves únicas
    this.uniqueKeyMap[tableName] = {};
    
    // Guardar cambios
    this.saveDatabase();
    
    return true;
  }

  /**
   * Elimina una tabla
   * @param {string} tableName - Nombre de la tabla
   * @returns {boolean} - Éxito de la operación
   */
  dropTable(tableName) {
    if (!this.data[tableName]) {
      return false;
    }
    
    // Eliminar la tabla
    delete this.data[tableName];
    
    // Limpiar índices y mapas
    delete this.indexes[tableName];
    delete this.uniqueKeyMap[tableName];
    delete this.primaryKeyMap[tableName];
    
    // Guardar cambios
    this.saveDatabase();
    
    return true;
  }

  /**
   * Crea una copia de seguridad de la base de datos
   * @returns {string} - Ruta del archivo de backup
   */
  backup() {
    const now = new Date();
    const backupFolder = `${path.dirname(this.config.file)}/backups`;
    
    // Crear carpeta de backups si no existe
    if (!fs.existsSync(backupFolder)) {
      fs.mkdirSync(backupFolder, { recursive: true });
    }
    
    // Generar nombre de archivo
    const timestamp = now.toISOString().replace(/[:.]/g, '-');
    const backupFilePath = `${backupFolder}/${path.basename(this.config.file, '.json')}_${timestamp}.json`;
    
    // Copiar archivo
    fs.copyFileSync(this.config.file, backupFilePath);
    
    console.log(`Backup creado: ${backupFilePath}`);
    
    return backupFilePath;
  }

  /**
   * Inicia el servicio de copias de seguridad automáticas
   */
  startBackupService() {
    if (!this.config.backups || this.config.backups.length === 0) {
      return;
    }
    
    this.config.backups.forEach((backupConfig) => {
      const interval = (backupConfig.interval || 1) * 24 * 60 * 60 * 1000; // Días a milisegundos
      
      setInterval(() => {
        try {
          this.backup();
        } catch (error) {
          console.error('Error al crear backup automático:', error);
        }
      }, interval);
      
      console.log(`Servicio de backup configurado cada ${backupConfig.interval} día(s)`);
    });
  }

  /**
   * Restaura la base de datos desde un backup
   * @param {string} backupFile - Ruta del archivo de backup
   * @returns {boolean} - Éxito de la operación
   */
  restore(backupFile) {
    try {
      if (!fs.existsSync(backupFile)) {
        throw new Error(`El archivo de backup '${backupFile}' no existe`);
      }
      
      // Crear backup del estado actual antes de restaurar
      this.backup();
      
      // Copiar archivo de backup a la ubicación de la base de datos
      fs.copyFileSync(backupFile, this.config.file);
      
      // Recargar la base de datos
      this.loadDatabase();
      
      return true;
    } catch (error) {
      console.error('Error al restaurar backup:', error);
      return false;
    }
  }

  /**
   * Obtiene todos los documentos de una tabla
   * @param {string} tableName - Nombre de la tabla
   * @returns {Array} - Documentos de la tabla
   */
  findAll(tableName) {
    if (!this.data[tableName]) {
      return [];
    }
    
    return [...this.data[tableName]];
  }

  /**
   * Busca documentos que cumplan con ciertos criterios
   * @param {string} tableName - Nombre de la tabla
   * @param {Object} query - Criterios de búsqueda
   * @returns {Array} - Documentos que cumplen los criterios
   */
  find(tableName, query) {
    if (!this.data[tableName]) {
      return [];
    }
    
    // Si no hay query, devolver todos
    if (!query || Object.keys(query).length === 0) {
      return this.findAll(tableName);
    }
    
    // Usar índice si es posible
    const singleFieldQuery = Object.keys(query).length === 1;
    const queryField = Object.keys(query)[0];
    
    if (singleFieldQuery && this.indexes[tableName] && this.indexes[tableName][queryField]) {
      const index = this.indexes[tableName][queryField][query[queryField]];
      if (index !== undefined) {
        return [this.data[tableName][index]];
      }
      return [];
    }
    
    // Búsqueda completa
    return this.data[tableName].filter(doc => this._matchesQuery(doc, query));
  }

  /**
   * Encuentra un documento por su clave primaria
   * @param {string} tableName - Nombre de la tabla
   * @param {string|number} id - Valor de la clave primaria
   * @returns {Object|null} - Documento encontrado o null
   */
  findById(tableName, id) {
    if (!this.data[tableName]) {
      return null;
    }
    
    const tableConfig = this.config.tables[tableName] || { primaryKey: 'id' };
    const primaryKey = tableConfig.primaryKey || 'id';
    
    // Usar índice si está disponible
    if (this.indexes[tableName] && this.indexes[tableName][primaryKey]) {
      const index = this.indexes[tableName][primaryKey][id];
      if (index !== undefined) {
        return { ...this.data[tableName][index] };
      }
      return null;
    }
    
    // Búsqueda lineal
    return this.data[tableName].find(doc => doc[primaryKey] === id) || null;
  }

  /**
   * Encuentra un documento que cumpla con ciertos criterios
   * @param {string} tableName - Nombre de la tabla
   * @param {Object} query - Criterios de búsqueda
   * @returns {Object|null} - Primer documento que cumple los criterios
   */
  findOne(tableName, query) {
    const results = this.find(tableName, query);
    return results.length > 0 ? results[0] : null;
  }

  /**
   * Obtiene una columna específica de todos los documentos
   * @param {string} tableName - Nombre de la tabla
   * @param {string} columnName - Nombre de la columna
   * @returns {Array} - Valores de la columna
   */
  getAllColumns(tableName, columnName) {
    if (!this.data[tableName]) {
      return [];
    }
    
    return this.data[tableName].map(row => row[columnName]);
  }

  /**
   * Cuenta el número de documentos en una tabla
   * @param {string} tableName - Nombre de la tabla
   * @param {Object} query - Criterios de búsqueda (opcional)
   * @returns {number} - Número de documentos
   */
  count(tableName, query = null) {
    if (!this.data[tableName]) {
      return 0;
    }
    
    if (!query) {
      return this.data[tableName].length;
    }
    
    return this.find(tableName, query).length;
  }

  /**
   * Obtiene el siguiente valor para la clave primaria
   * @param {string} tableName - Nombre de la tabla
   * @returns {number} - Siguiente valor
   * @private
   */
  getNextPrimaryKey(tableName) {
    if (!this.primaryKeyMap[tableName]) {
      this._calculateNextPrimaryKey(tableName, this.config.tables[tableName]?.primaryKey || 'id');
    }
    
    const nextKey = this.primaryKeyMap[tableName];
    this.primaryKeyMap[tableName]++;
    
    return nextKey;
  }

  /**
   * Verifica si un documento cumple con los criterios de búsqueda
   * @param {Object} doc - Documento a verificar
   * @param {Object} query - Criterios de búsqueda
   * @returns {boolean} - Si el documento cumple los criterios
   * @private
   */
  _matchesQuery(doc, query) {
    for (const field in query) {
      const queryValue = query[field];
      
      // Manejar operadores especiales
      if (typeof queryValue === 'object' && queryValue !== null) {
        if (!this._matchesOperators(doc[field], queryValue)) {
          return false;
        }
      } 
      // Comparación directa
      else if (doc[field] !== queryValue) {
        return false;
      }
    }
    
    return true;
  }

  /**
   * Verifica si un valor cumple con los operadores especiales
   * @param {any} value - Valor a verificar
   * @param {Object} operators - Operadores de comparación
   * @returns {boolean} - Si el valor cumple con los operadores
   * @private
   */
  _matchesOperators(value, operators) {
    for (const op in operators) {
      const opValue = operators[op];
      
      switch (op) {
        case '$gt':
          if (!(value > opValue)) return false;
          break;
        case '$gte':
          if (!(value >= opValue)) return false;
          break;
        case '$lt':
          if (!(value < opValue)) return false;
          break;
        case '$lte':
          if (!(value <= opValue)) return false;
          break;
        case '$ne':
          if (value === opValue) return false;
          break;
        case '$in':
          if (!Array.isArray(opValue) || !opValue.includes(value)) return false;
          break;
        case '$nin':
          if (!Array.isArray(opValue) || opValue.includes(value)) return false;
          break;
      }
    }
    
    return true;
  }

  /**
   * Verifica las restricciones de unicidad
   * @param {string} tableName - Nombre de la tabla
   * @param {Object} data - Datos a verificar
   * @returns {boolean} - Si los datos cumplen las restricciones
   * @private
   */
  _checkUniqueConstraints(tableName, data) {
    const tableConfig = this.config.tables[tableName];
    if (!tableConfig) return true;
    
    const uniqueConstraints = tableConfig.unique || [];
    
    for (const uniqueKey of uniqueConstraints) {
      // Ignorar si el campo no tiene valor
      if (data[uniqueKey] === undefined || data[uniqueKey] === null) {
        continue;
      }
      
      // Verificar si ya existe
      if (this.uniqueKeyMap[tableName] && 
          this.uniqueKeyMap[tableName][uniqueKey] && 
          this.uniqueKeyMap[tableName][uniqueKey].has(data[uniqueKey])) {
        return false;
      }
    }
    
    return true;
  }

  /**
   * Verifica las restricciones de unicidad para actualización
   * @param {string} tableName - Nombre de la tabla
   * @param {Object} newData - Nuevos datos
   * @param {Object} originalData - Datos originales
   * @returns {boolean} - Si los datos cumplen las restricciones
   * @private
   */
  _checkUniqueConstraintsForUpdate(tableName, newData, originalData) {
    const tableConfig = this.config.tables[tableName];
    if (!tableConfig) return true;
    
    const uniqueConstraints = tableConfig.unique || [];
    
    for (const uniqueKey of uniqueConstraints) {
      // Si el valor no cambió, está bien
      if (newData[uniqueKey] === originalData[uniqueKey]) {
        continue;
      }
      
      // Ignorar si el campo no tiene valor
      if (newData[uniqueKey] === undefined || newData[uniqueKey] === null) {
        continue;
      }
      
      // Verificar si ya existe
      if (this.uniqueKeyMap[tableName] && 
          this.uniqueKeyMap[tableName][uniqueKey] && 
          this.uniqueKeyMap[tableName][uniqueKey].has(newData[uniqueKey])) {
        return false;
      }
    }
    
    return true;
  }

  /**
   * Actualiza los índices con un nuevo documento
   * @param {string} tableName - Nombre de la tabla
   * @param {Object} doc - Documento
   * @param {number} docIndex - Índice del documento
   * @private
   */
  _updateIndexes(tableName, doc, docIndex) {
    if (!this.indexes[tableName]) {
      this.indexes[tableName] = {};
    }
    
    // Actualizar índices para cada campo indexado
    for (const field in this.indexes[tableName]) {
      if (doc[field] !== undefined) {
        this.indexes[tableName][field][doc[field]] = docIndex;
      }
    }
  }

  /**
   * Elimina un documento de los índices
   * @param {string} tableName - Nombre de la tabla
   * @param {Object} doc - Documento
   * @private
   */
  _removeFromIndexes(tableName, doc) {
    if (!this.indexes[tableName]) return;
    
    // Eliminar de cada índice
    for (const field in this.indexes[tableName]) {
      if (doc[field] !== undefined) {
        delete this.indexes[tableName][field][doc[field]];
      }
    }
  }

  /**
   * Actualiza los mapas de claves únicas
   * @param {string} tableName - Nombre de la tabla
   * @param {Object} data - Datos a añadir
   * @private
   */
  _updateUniqueKeyMap(tableName, data) {
    const tableConfig = this.config.tables[tableName];
    if (!tableConfig) return;
    
    const uniqueConstraints = tableConfig.unique || [];
    
    // Inicializar si no existe
    if (!this.uniqueKeyMap[tableName]) {
      this.uniqueKeyMap[tableName] = {};
    }
    
    // Añadir a cada Set de restricción única
    for (const uniqueKey of uniqueConstraints) {
      if (!this.uniqueKeyMap[tableName][uniqueKey]) {
        this.uniqueKeyMap[tableName][uniqueKey] = new Set();
      }
      
      if (data[uniqueKey] !== undefined && data[uniqueKey] !== null) {
        this.uniqueKeyMap[tableName][uniqueKey].add(data[uniqueKey]);
      }
    }
  }

  /**
   * Actualiza los mapas de claves únicas para actualización
   * @param {string} tableName - Nombre de la tabla
   * @param {Object} newData - Nuevos datos
   * @param {Object} originalData - Datos originales
   * @private
   */
  _updateUniqueKeyMapForUpdate(tableName, newData, originalData) {
    const tableConfig = this.config.tables[tableName];
    if (!tableConfig) return;
    
    const uniqueConstraints = tableConfig.unique || [];
    
    for (const uniqueKey of uniqueConstraints) {
      // Si el valor cambió, actualizar los sets
      if (newData[uniqueKey] !== originalData[uniqueKey]) {
        // Eliminar el valor antiguo
        if (originalData[uniqueKey] !== undefined && originalData[uniqueKey] !== null) {
          this.uniqueKeyMap[tableName][uniqueKey].delete(originalData[uniqueKey]);
        }
        
        // Añadir el nuevo valor
        if (newData[uniqueKey] !== undefined && newData[uniqueKey] !== null) {
          this.uniqueKeyMap[tableName][uniqueKey].add(newData[uniqueKey]);
        }
      }
    }
  }

  /**
   * Elimina un documento de los mapas de claves únicas
   * @param {string} tableName - Nombre de la tabla
   * @param {Object} doc - Documento
   * @private
   */
  _removeFromUniqueKeyMap(tableName, doc) {
    const tableConfig = this.config.tables[tableName];
    if (!tableConfig) return;
    
    const uniqueConstraints = tableConfig.unique || [];
    
    for (const uniqueKey of uniqueConstraints) {
      if (this.uniqueKeyMap[tableName] && 
          this.uniqueKeyMap[tableName][uniqueKey] && 
          doc[uniqueKey] !== undefined) {
        this.uniqueKeyMap[tableName][uniqueKey].delete(doc[uniqueKey]);
      }
    }
  }

  /**
   * Ejecuta una transacción
   * @param {Function} transactionFn - Función de transacción
   * @returns {any} - Resultado de la transacción
   */
  transaction(transactionFn) {
    // Guardar estado actual
    const currentState = JSON.stringify(this.data);
    
    try {
      // Ejecutar transacción
      const result = transactionFn(this);
      
      // Guardar cambios
      this.saveDatabase();
      
      return result;
    } catch (error) {
      // Restaurar estado anterior en caso de error
      this.data = JSON.parse(currentState);
      
      console.error('Error en transacción, rollback realizado:', error);
      throw error;
    }
  }

  /**
   * Ejecuta una consulta agregada
   * @param {string} tableName - Nombre de la tabla
   * @param {Array} pipeline - Pipeline de agregación
   * @returns {Array} - Resultados de la agregación
   */
  aggregate(tableName, pipeline) {
    if (!this.data[tableName]) {
      return [];
    }
    
    let result = [...this.data[tableName]];
    
    // Procesar cada etapa del pipeline
    for (const stage of pipeline) {
      // $match - filtrado similar a find()
      if (stage.$match) {
        result = result.filter(doc => this._matchesQuery(doc, stage.$match));
      }
      
      // $project - selección de campos
      if (stage.$project) {
        result = result.map(doc => {
          const projected = {};
          for (const field in stage.$project) {
            if (stage.$project[field] === 1) {
              projected[field] = doc[field];
            }
          }
          return projected;
        });
      }
      
      // $group - agrupación
      if (stage.$group) {
        const groups = {};
        const idField = stage.$group._id;
        
        result.forEach(doc => {
          // Determinar clave de grupo
          let groupKey;
          if (typeof idField === 'string' && idField.startsWith('$')) {
            // $group: { _id: "$campo" }
            const fieldName = idField.substring(1);
            groupKey = doc[fieldName];
          } else {
            // Grupo único
            groupKey = '_all';
          }
          
          // Convertir a string para usar como clave
          const groupKeyStr = String(groupKey);
          
          // Inicializar grupo si no existe
          if (!groups[groupKeyStr]) {
            groups[groupKeyStr] = { _id: groupKey };
            
            // Inicializar acumuladores
            for (const field in stage.$group) {
              if (field !== '_id') {
                const accumulator = stage.$group[field];
                
                if (accumulator.$sum) {
                  groups[groupKeyStr][field] = 0;
                } else if (accumulator.$avg) {
                  groups[groupKeyStr][field] = { sum: 0, count: 0 };
                } else if (accumulator.$min) {
                  groups[groupKeyStr][field] = Infinity;
                } else if (accumulator.$max) {
                  groups[groupKeyStr][field] = -Infinity;
                } else if (accumulator.$push) {
                  groups[groupKeyStr][field] = [];
                }
              }
            }
          }
          
          // Actualizar acumuladores
          for (const field in stage.$group) {
            if (field !== '_id') {
              const accumulator = stage.$group[field];
              
              if (accumulator.$sum) {
                const valueField = accumulator.$sum;
                if (valueField === 1) {
                  groups[groupKeyStr][field] += 1;
                } else if (typeof valueField === 'string' && valueField.startsWith('$')) {
                  const fieldName = valueField.substring(1);
                  groups[groupKeyStr][field] += (doc[fieldName] || 0);
                }
              } else if (accumulator.$avg) {
                const valueField = accumulator.$avg;
                if (typeof valueField === 'string' && valueField.startsWith('$')) {
                  const fieldName = valueField.substring(1);
                  groups[groupKeyStr][field].sum += (doc[fieldName] || 0);
                  groups[groupKeyStr][field].count += 1;
                }
              } else if (accumulator.$min) {
                const valueField = accumulator.$min;
                if (typeof valueField === 'string' && valueField.startsWith('$')) {
                  const fieldName = valueField.substring(1);
                  groups[groupKeyStr][field] = Math.min(groups[groupKeyStr][field], doc[fieldName] || Infinity);
                }
              } else if (accumulator.$max) {
                const valueField = accumulator.$max;
                if (typeof valueField === 'string' && valueField.startsWith('$')) {
                  const fieldName = valueField.substring(1);
                  groups[groupKeyStr][field] = Math.max(groups[groupKeyStr][field], doc[fieldName] || -Infinity);
                }
              } else if (accumulator.$push) {
                const valueField = accumulator.$push;
                if (typeof valueField === 'string' && valueField.startsWith('$')) {
                  const fieldName = valueField.substring(1);
                  groups[groupKeyStr][field].push(doc[fieldName]);
                }
              }
            }
          }
        });
        
        // Finalizar acumuladores
        result = Object.values(groups).map(group => {
          for (const field in stage.$group) {
            if (field !== '_id') {
              const accumulator = stage.$group[field];
              
              if (accumulator.$avg) {
                group[field] = group[field].sum / (group[field].count || 1);
              }
            }
          }
          return group;
        });
      }
      
      // $sort - ordenamiento
      if (stage.$sort) {
        result.sort((a, b) => {
          for (const field in stage.$sort) {
            const dir = stage.$sort[field];
            if (a[field] < b[field]) return -1 * dir;
            if (a[field] > b[field]) return 1 * dir;
          }
          return 0;
        });
      }
      
      // $limit - limitar resultados
      if (stage.$limit) {
        result = result.slice(0, stage.$limit);
      }
      
      // $skip - saltar resultados
      if (stage.$skip) {
        result = result.slice(stage.$skip);
      }
    }
    
    return result;
  }

  /**
   * Crea un índice en un campo
   * @param {string} tableName - Nombre de la tabla
   * @param {string} field - Campo a indexar
   * @returns {boolean} - Éxito de la operación
   */
  createIndex(tableName, field) {
    if (!this.data[tableName]) {
      return false;
    }
    
    this._createIndex(tableName, field);
    
    return true;
  }

  /**
   * Elimina un índice
   * @param {string} tableName - Nombre de la tabla
   * @param {string} field - Campo indexado
   * @returns {boolean} - Éxito de la operación
   */
  dropIndex(tableName, field) {
    if (!this.indexes[tableName] || !this.indexes[tableName][field]) {
      return false;
    }
    
    delete this.indexes[tableName][field];
    
    return true;
  }

  /**
   * Obtiene estadísticas de la base de datos
   * @returns {Object} - Estadísticas
   */
  stats() {
    const stats = {
      tables: {},
      totalDocuments: 0,
      databaseSize: 0,
      indexes: {}
    };
    
    // Estadísticas por tabla
    for (const tableName in this.data) {
      stats.tables[tableName] = {
        documents: this.data[tableName].length,
        indexes: Object.keys(this.indexes[tableName] || {})
      };
      
      stats.totalDocuments += this.data[tableName].length;
    }
    
    // Tamaño aproximado
    try {
      const fileStats = fs.statSync(this.config.file);
      stats.databaseSize = fileStats.size;
    } catch (error) {
      stats.databaseSize = 0;
    }
    
    // Estadísticas de índices
    for (const tableName in this.indexes) {
      stats.indexes[tableName] = Object.keys(this.indexes[tableName]).length;
    }
    
    return stats;
  }

  /**
   * Reinicia completamente la base de datos
   * @returns {boolean} - Éxito de la operación
   */
  reset() {
    try {
      // Crear backup antes de reiniciar
      this.backup();
      
      // Reiniciar estado
      this.data = {};
      this.primaryKeyMap = {};
      this.uniqueKeyMap = {};
      this.indexes = {};
      
      // Guardar cambios
      this.saveDatabase();
      
      return true;
    } catch (error) {
      console.error('Error al reiniciar la base de datos:', error);
      return false;
    }
  }

  /**
   * Importa datos de un archivo JSON
   * @param {string} filePath - Ruta del archivo
   * @param {Object} options - Opciones de importación
   * @returns {boolean} - Éxito de la operación
   */
  importFromJson(filePath, options = {}) {
    try {
      const rawData = fs.readFileSync(filePath, 'utf8');
      const importData = JSON.parse(rawData);
      
      // Determinar modo de importación
      const mode = options.mode || 'merge'; // 'merge' o 'replace'
      
      if (mode === 'replace') {
        // Reemplazar toda la base de datos
        this.data = importData;
      } else {
        // Fusionar con datos existentes
        for (const tableName in importData) {
          if (!this.data[tableName]) {
            this.data[tableName] = [];
          }
          
          // Añadir documentos
          importData[tableName].forEach(doc => {
            // Verificar si ya existe un documento con la misma clave primaria
            const primaryKey = this.config.tables[tableName]?.primaryKey || 'id';
            const existingIndex = this.data[tableName].findIndex(item => item[primaryKey] === doc[primaryKey]);
            
            if (existingIndex === -1) {
              // No existe, añadir
              this.data[tableName].push(doc);
            } else if (options.overwrite) {
              // Existe y se debe sobreescribir
              this.data[tableName][existingIndex] = doc;
            }
            // Si no se debe sobreescribir, ignorar
          });
        }
      }
      
      // Reconstruir índices y mapas
      this._initializeUniqueKeyMaps();
      this._initializeIndexes();
      
      // Guardar cambios
      this.saveDatabase();
      
      return true;
    } catch (error) {
      console.error('Error al importar datos:', error);
      return false;
    }
  }

  /**
   * Exporta la base de datos a un archivo JSON
   * @param {string} filePath - Ruta del archivo
   * @param {Object} options - Opciones de exportación
   * @returns {boolean} - Éxito de la operación
   */
  exportToJson(filePath, options = {}) {
    try {
      let exportData = this.data;
      
      // Exportar tablas específicas
      if (options.tables && Array.isArray(options.tables)) {
        exportData = {};
        options.tables.forEach(tableName => {
          if (this.data[tableName]) {
            exportData[tableName] = this.data[tableName];
          }
        });
      }
      
      // Excluir metadatos si se solicita
      if (options.excludeMetadata) {
        exportData = JSON.parse(JSON.stringify(exportData));
        
        for (const tableName in exportData) {
          exportData[tableName] = exportData[tableName].map(doc => {
            const cleaned = { ...doc };
            delete cleaned._createdAt;
            delete cleaned._updatedAt;
            return cleaned;
          });
        }
      }
      
      // Escribir a archivo
      fs.writeFileSync(filePath, JSON.stringify(exportData, null, 2));
      
      return true;
    } catch (error) {
      console.error('Error al exportar datos:', error);
      return false;
    }
  }
}

// Exportar clase y tipos
module.exports = {
  DataOrbit,
  DataType
};
