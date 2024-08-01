const fs = require('fs');
const DataType = {

    TEXT: 'Text',

    NUMBER: 'Number',

    PATH: 'Path' 

};
class DataOrbit {

    constructor(config) {

        this.config = config;

        this.data = {};

        this.primaryKeyMap = {};

        this.uniqueKeyMap = {};

        this.loadDatabase();

    }

    loadDatabase() {

        try {

            const rawData = fs.readFileSync(this.config.file, 'utf8');

            if (rawData.trim() !== '') {

                const decryptedData = this.decryptData(rawData, this.config.encryptionKey);

                this.data = JSON.parse(decryptedData);

            } else {

                console.log('El archivo JSON está vacío.');

            }

        } catch (error) {

            console.error('Error loading database:', error);

        }

    }

    saveDatabase() {

        try {

            const encryptedData = this.encryptData(JSON.stringify(this.data, null, 4), this.config.encryptionKey);

            fs.writeFileSync(this.config.file, encryptedData);

        } catch (error) {

            console.error('Error saving database:', error);

        }

    }

    encryptData(data, encryptionKey) {

        let encrypted = '';

        for (let i = 0; i < data.length; i++) {

            encrypted += String.fromCharCode(data.charCodeAt(i) ^ encryptionKey.charCodeAt(i % encryptionKey.length));

        }

        return encrypted;

    }

    decryptData(data, encryptionKey) {

        return this.encryptData(data, encryptionKey);

    }

    insert(tableName, data) {

        if (!this.data[tableName]) {

            this.data[tableName] = [];

        }

        const primaryKey = this.config.tables[tableName].primaryKey || 'id';

        if (!data[primaryKey]) {

            data[primaryKey] = this.getNextPrimaryKey(tableName);

            if (!this.checkUniqueConstraints(tableName, data)) {

                console.error('Error: Unique constraint violation.');

                return;

            }

            this.data[tableName].push(data);

            this.saveDatabase();

        }

    }

    delete(tableName, primaryKey) {

        if (this.data[tableName]) {

            this.data[tableName] = this.data[tableName].filter(item => item[primaryKey] !== data[primaryKey]);

            this.saveDatabase();

        }

    }

    editInfo(tableName, primaryKey, newData) {

        if (this.data[tableName]) {

            this.data[tableName] = this.data[tableName].map(item => {

                if (item[primaryKey] === newData[primaryKey]) {

                    return { ...item, ...newData };

                }

                return item;

            });

            this.saveDatabase();

        }

    }

    createTable(tableName, schema) {

        if (!this.data[tableName]) {

            this.data[tableName] = [];

            this.saveDatabase();

        }

    }

    dropTable(tableName) {

        delete this.data[tableName];

        this.saveDatabase();

    }

    backup() {

        const now = new Date();

        const backupFolder = `${this.config.file}_backups`;

        if (!fs.existsSync(backupFolder)) {

            fs.mkdirSync(backupFolder);

        }

        const backupFilePath = `${backupFolder}/${now.getTime()}_backup.json`;

        fs.copyFileSync(this.config.file, backupFilePath);

    }

    startBackupService() {

        this.config.backups.forEach((backupConfig) => {

            setInterval(() => {

                this.backup();

            }, backupConfig.interval * 24 * 60 * 60 * 1000); 

        });

    }

    getAllRows(tableName) {

        if (this.data[tableName]) {

            return this.data[tableName];

        }

        return [];

    }

    getAllColumns(tableName, columnName) {

        if (this.data[tableName]) {

            return this.data[tableName].map(row => row[columnName]);

        }

        return [];

    }

    getRow(tableName, primaryKey, value) {

        if (this.data[tableName]) {

            return this.data[tableName].find(row => row[primaryKey] === value) || null;

        }

        return null;

    }

    getColumn(tableName, primaryKey) {

        if (this.data[tableName]) {

            return this.data[tableName].map(row => row[primaryKey]);

        }

        return [];

    }

    getNextPrimaryKey(tableName) {

        if (!this.primaryKeyMap[tableName]) {

            this.primaryKeyMap[tableName] = 1;

        } else {

            this.primaryKeyMap[tableName]++;

        }

        return this.primaryKeyMap[tableName];

    }

    checkUniqueConstraints(tableName, data) {

        const uniqueConstraints = this.config.tables[tableName].unique || [];

        for (const uniqueKey of uniqueConstraints) {

            if (this.uniqueKeyMap[tableName][uniqueKey].has(data[uniqueKey])) {

                return false;

            }

        }

        return true;

    }

    updateUniqueKeyMap(tableName, data) {

        const uniqueConstraints = this.config.tables[tableName].unique || [];

        for (const uniqueKey of uniqueConstraints) {

            if (!this.uniqueKeyMap[tableName]) {

                this.uniqueKeyMap[tableName] = {};

            }

            if (!this.uniqueKeyMap[tableName][uniqueKey]) {

                this.uniqueKeyMap[tableName][uniqueKey] = new Set();

            }

            this.uniqueKeyMap[tableName][uniqueKey].add(data[uniqueKey]);

        }

    }

    editInfo(tableName, primaryKey, newData) {
  if (this.data[tableName]) {
    const index = this.data[tableName].findIndex(item => item[primaryKey] === newData[primaryKey]);
    if (index !== -1) {
      this.data[tableName][index] = { ...this.data[tableName][index], ...newData };
      this.saveDatabase();
    }
  }
}
}

module.exports = DataOrbit;