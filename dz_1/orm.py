import pymysql
import inspect

conn = pymysql.connect(host='localhost',
                       user='root',
                       password='',
                       db='advanced_python')


def query_set(query, outline=None, flag=True):
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
    if table is None and flag==True:
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

        try:
            self.f_type(value)
        except:
            raise TypeError
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

        # попытка сделать mro
        for base in bases:
            for m in base.__mro__:
                if m == eval('Model'):
                    break
                else:
                    namespace = {**m.__dict__, **namespace}
        #

        fields = {k: v for k, v in namespace.items()
                  if isinstance(v, Field)}
        namespace['_fields'] = fields
        namespace['_table_name'] = meta.table_name
        return super().__new__(mcs, name, bases, namespace)


class Manage:

    def __init__(self):
        self.model_cls = None
        self.list_object = []

    def __get__(self, instance, owner):
        # if self.model_cls is None:
        #     self.model_cls = owner
        self.model_obj = instance
        self.model_cls = owner
        return self

    def __getitem__(self, item):
        if type(item) == slice:
            self.list_object = self.list_object[item]
        else:
            self.item = item
            if item >= len(self.list_object):
                return None
            return self.list_object[item]

    def __iter__(self):
        self.item = -1
        return self

    def __next__(self):
        self.item = self.item + 1
        if self.item == len(self.list_object):
            raise StopIteration
        else:
            return self.list_object[self.item]

    def all(self):
        query = 'SELECT * FROM {} '.format(self.model_cls.Meta.table_name)
        tables = query_set(query, 'many')
        dict_table = {}  # поле и значение объекта
        for table_index in range(len(tables)):
            for key, val in zip(self.model_cls._fields.keys(), tables[table_index]):
                dict_table[key] = val

            self.list_object.append(self.model_cls(**dict_table))
            for field_name, field in self.model_cls._fields.items():
                value = field.validate(dict_table.get(field_name))
                setattr(self.list_object[table_index], field_name, value)
        return self

    def get(self, id, flag=True):
        kwargs = {}
        query = "SELECT * FROM {} WHERE id={}".format(self.model_cls.Meta.table_name, id)
        table = query_set(query, 'one', flag)
        if table is None:
            return None
        for key, val in zip(self.model_cls._fields.keys(), table):
            kwargs[key] = val
        return self.model_cls(**kwargs)

    def create(self, **kwargs):
        for field_name, field in self.model_cls._fields.items():
            value = field.validate(kwargs.get(field_name))
            setattr(self.model_cls, field_name, value)
            kwargs[field_name] = value
        query = "INSERT INTO {} ({}) VALUES ({})".format(self.model_cls.Meta.table_name,
                                                         ', '.join(str(x) for x in kwargs.keys()), ', '.join(
                str(x) if type(x) != str else r"'" + x + r"'" for x in kwargs.values()))
        query_set(query)

    def save(self):
        if len(self.list_object) != 0 and self.model_obj is None:  # если не одна, обновить каждую строку
            for i in self.list_object:
                i.save()
        field_names = {}
        for field_name in self.model_cls._fields.keys():
            field_names[field_name] = ''
        if self.model_obj is not None:
            for key in field_names.keys():
                field_names[key] = getattr(self.model_obj, key)
        else:
            return None
        if self.get(id=self.model_obj.id, flag=False) == None:
            self.create(**field_names)
        else:
            self.update(**field_names)

    def delete(self):
        if len(self.list_object) != 0 and self.model_obj is None:  # если не одна, удалить каждую строку
            i = 0
            while len(self.list_object) != 0:
                self.list_object[i].delete()
                del self.list_object[i]

        id_column = self.model_obj.id

        query = "DELETE FROM {} WHERE ({})".format(self.model_cls.Meta.table_name,
                                                   'id={}'.format(id_column))
        query_set(query)
        return None

    def update(self, *_, **kwargs):
        if len(self.list_object) != 0 and self.model_obj is None:  # если не одна, обновить каждую строку
            for i in self.list_object:
                i.update(**kwargs)
        elif self.model_obj is None:
            return
        set_value = ', '.join("{}='{}'".format(x, y) if isinstance(y, str) else '{}={}'.format(x, y)
                              for x, y in kwargs.items())
        condition = 'id={}'.format(self.model_obj.id)
        query = "UPDATE {} SET  {} WHERE {}".format(self.model_cls.Meta.table_name, set_value, condition)
        query_set(query)
        for field_name, value in kwargs.items():
            setattr(self.model_obj, field_name, value)

    def filter(self, **kwargs):
        new_list_object = []
        for model_obj in self.list_object:
            for field_name, value in kwargs.items():
                if getattr(model_obj, field_name) != value:
                    if model_obj in new_list_object:
                        new_list_object.remove(model_obj)
                    break
                else:
                    if model_obj in new_list_object:
                        continue
                    new_list_object.append(model_obj)
        self.list_object = new_list_object
        return self


class Model(metaclass=ModelMeta):
    class Meta:
        table_name = ''

    objects = Manage()

    def __init__(self, *_, **kwargs):
        for field_name, field in self._fields.items():
            value = field.validate(kwargs.get(field_name))
            setattr(self, field_name, value)

    def save(self):
        return self.objects.save()

    def delete(self):
        return self.objects.delete()

    def update(self, **kwargs):
        return self.objects.update(**kwargs)


class User(Model):
    id = IntField()
    name = StringField()
    sex = BoolField()
    height = FloatField()

    class Meta:
        table_name = 'Users'


class Man(User, Model):
    age = IntField()

    class Meta:
        table_name = 'Man'


# Создание строки в таблице
# User.objects.create(id=1, name='Иванов', sex=True, height=180.1)
# User.objects.create(id=2, name='Иванова', sex=False, height=155.0)
# User.objects.create(id=3, name='Сидоров', sex=True, height=170.5)
# User.objects.create(id=5, name='Иванов', sex=False, height=160.0)

#
# Создание экземпляра
#
# user = User(id=4, name='Петров', sex=True, height=147.1)
# user.save()
#
# user = User(id=4, name='Петров', sex=True, height=147.1)
# user.name='Change'
# user.save()
#
# Извлечение данных через get
#
# user = User.objects.get(id=4)
# print(user.__dict__)
# user.name = 'Change2'
# user.save()
# user2 = User.objects.get(id=4)
# print(user2.__dict__)
#
# man = Man.objects.get(id=1)
# print(man)
# print(man.__dict__)
#
#
# Извлечение данных через .all()
#
# user = User.objects.all()
# user = user.filter(name='Иванова').filter(id=2).update(name='Иванов')
# user.filter(name='Change2').delete()
#
#
# users = User.objects.all().filter(name='Иванов')
#
# for u in users:
#     print(u.id, u.name)
#
# Метод обновления update()
#
# user = User.objects.get(id=1)
# user.update(name='Change2')
#
# для delete
#
# user = User.objects.get(id=1)
# user.delete()
#

# man = Man.objects.create(id=1, name=12, sex='0', height=180.1, age=22)

conn.close()