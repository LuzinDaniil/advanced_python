import pymysql

conn = pymysql.connect(host='localhost',
                       user='root',
                       password='1',
                       db='advanced_python')


def query_set(query, outline=None):
    with conn.cursor() as cursor:
        try:
            cursor.execute(query)
        except (pymysql.err.OperationalError, pymysql.ProgrammingError, pymysql.InternalError,
                pymysql.IntegrityError, TypeError) as err:
            print('Database reset error: ', err.args[1])
        if outline == 'one':
            table = cursor.fetchone()
        elif outline == 'many':
            table = cursor.fetchall()
        else:
            conn.commit()
            return None
    if table is None:
        print('TypeError: Database reset None')
    return table


class Field:
    def __init__(self, f_type, required=True, default=None):
        self.f_type = f_type
        self.required = required
        self.default = default

    def validate(self, value):
        if value is None and not self.required:
            return None

        # todo exceptions
        return self.f_type(value)


class IntField(Field):
    def __init__(self, required=True, default=None):
        super().__init__(int, required, 0)


class StringField(Field):
    def __init__(self, required=True, default=None):
        super().__init__(str, required, '')


class BoolField(Field):
    def __init__(self, required=True, default=None):
        super().__init__(bool, required, False)


class FloatField(Field):
    def __init__(self, required=True, default=None):
        super().__init__(float, required, 0)


class ModelMeta(type):
    def __new__(mcs, name, bases, namespace):
        if name == 'Model':
            return super().__new__(mcs, name, bases, namespace)

        meta = namespace.get('Meta')
        if meta is None:
            raise ValueError('meta is none')
        if not hasattr(meta, 'table_name'):
            raise ValueError('table_name is empty')

        # todo mro

        fields = {k: v for k, v in namespace.items()
                  if isinstance(v, Field)}
        namespace['_fields'] = fields
        namespace['_table_name'] = meta.table_name
        mcs.container = {}
        return super().__new__(mcs, name, bases, namespace)

    # для возможности индексации
    def __setitem__(self, key, value):
        self.container[key] = value

    def __getitem__(self, item):
        self.item = item
        if item >= len(self.container):
            return None
        return self.container[item]

    # для итерирования
    def __iter__(self):
        self.item = -1
        return self

    def __next__(self):
        self.item = self.item + 1
        if self.item == len(self.container):
            raise StopIteration
        else:
            return self.container[self.item]


class Manage:
    def __init__(self):
        self.model_cls = None

    def __get__(self, instance, owner):
        if self.model_cls is None:
            self.model_cls = owner
        self.instance_1 = instance
        return self

    def all(self, condition=None):
        query = 'SELECT * FROM {} '.format(self.model_cls.Meta.table_name)
        if condition is not None:
            query = query + " WHERE ({})".format(
                ' and '.join(y + r"=" + str(x) + r"" if type(x) != str else y + r"='" + x + r"'" for y, x in
                             condition.items()))
        if condition is not None:
            tables = query_set(query, 'one')
        else:
            tables = query_set(query, 'many')
        dict_table = {}
        for table_index in range(len(tables)):
            for key, val in zip(self.model_cls._fields.keys(), tables[table_index]):
                dict_table[key] = val
            self.model_cls[table_index] = self.model_cls(**dict_table)
            for field_name, field in self.model_cls._fields.items():
                value = field.validate(dict_table.get(field_name))
                setattr(self.model_cls[table_index], field_name, value)
        return self.model_cls

    def get(self, *args, **kwargs):
        query = "SELECT * FROM {} WHERE id={}".format(self.model_cls.Meta.table_name, kwargs['id'])
        table = query_set(query, 'one')
        if table is None:
            return None
        for key, val in zip(self.model_cls._fields.keys(), table):
            kwargs[key] = val
        return self.model_cls(**kwargs)

    def create(self, *args, **kwargs):
        for field_name, field in self.model_cls._fields.items():
            value = field.validate(kwargs.get(field_name))
            setattr(self.model_cls, field_name, value)
            kwargs[field_name] = value
        query = "INSERT INTO {} ({}) VALUES ({})".format(self.model_cls.Meta.table_name,
                                                         ', '.join(str(x) for x in kwargs.keys()), ', '.join(
                str(x) if type(x) != str else r"'" + x + r"'" for x in kwargs.values()))
        query_set(query)

    def save(self, *args):
        field_names = {}
        for field_name in self.model_cls._fields.keys():
            field_names[field_name] = ''
        if len(self.model_cls) == 0:
            for key in field_names.keys():
                field_names[key] = getattr(self.model_cls, key)
            query = "UPDATE {} SET {}".format(self.model_cls.Meta.table_name,
                                              ', '.join(
                                                  y + r"=" + str(x) if type(x) != str else y + r"='" + x + r"'" for y, x
                                                  in field_names.items())
                                              )
            query = query + " WHERE id={}".format(self.model_cls.id)
            query_set(query)
            return


class Model(metaclass=ModelMeta):
    class Meta:
        table_name = ''

    objects = Manage()

    # todo DoesNotExist

    def __init__(self, *_, **kwargs):
        for field_name, field in self._fields.items():
            value = field.validate(kwargs.get(field_name))
            setattr(self, field_name, value)

    def save(self):
        field_names = {}
        for field_name in self._fields.keys():
            field_names[field_name] = ''
        for key in field_names.keys():
            field_names[key] = getattr(self, key)
        query = "INSERT INTO {} ({}) VALUES ({})".format(self.Meta.table_name,
                                                         ', '.join(str(x) for x in field_names.keys()), ', '.join(
                str(x) if type(x) != str else r"'" + x + r"'" for x in field_names.values()))
        query_set(query)

    def delete(self):
        id_column = self.id
        query = "DELETE FROM {} WHERE ({})".format(self.Meta.table_name,
                                                   'id={}'.format(id_column))
        query_set(query)
        for field_name, field in self._fields.items():
            setattr(self, field_name, None)

    def update(self, *_, **kwargs):
        id_column = self.id
        query = "UPDATE {} SET {} WHERE {}".format(self.Meta.table_name,
                                                   ', '.join(
                                                       y + r"=" + str(x) if type(x) != str else y + r"='" + x + r"'" for
                                                       y, x
                                                       in kwargs.items()), 'id={}'.format(id_column)
                                                   )
        query_set(query)


class User(Model):
    id = IntField()
    name = StringField()
    sex = BoolField()
    height = FloatField()

    class Meta:
        table_name = 'Users'



class Man(User):
    age = IntField()

    class Meta:
        table_name = 'Man'


# Создание строки в таблице
# User.objects.create(id=1, name=12, sex='0', height=180.1)
#
# Создание экземпляра
#
# user = User(id=2, name='Иванова', sex='1', height=147.1)
# print(user.name)
# user.save()
#
# Извлечение данных через get
#
# user = User.objects.get(id=2)
# print(user.name)
#
#
# Извлечение данных через .all()
#
user = User.objects.all()

print(user[2].name)
for i in user:
    print(i.name)
#
# Метод обновления update()
#
# user = User.objects.get(id=1)
# user.update(name='Change')
#
# для delete
#
# user = User.objects.get(id=1)
# user.delete()
#

# man = Man.objects.create(id=1, name=12, sex='0', height=180.1, age=22)


conn.close()
