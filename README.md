# Bank System in Python with OOP in SQL database
*This is a challenge of Data Engineering NTT DATA Bootcamp in [DIO](https://web.dio.me/).* It consists in a bank system based in five operations: withdraw money, deposit money, see account statement, register user and register account; using OOP in a SQL database. The project also considers the currency as the brazilian one (Real).

# The Libraries
It was used ```Pandas``` to generate statement table, ```sqlite3``` to create database file and ```datetime``` module to create a date.

# Rules

## Withdraw rules
- A person can not withdraw if there is no money in account;
- A person can not withdraw if desired value is higher than account balance;

## Deposit rule
- Deposit value can not be negative

## Statement rule
- The table must describe the operation executed and its value;
- The values in table must be like R$ XX.XX (R$ 34.56, R$ 123.56)

## Registers rules
- An user must be registered if wasn't registered before;
- An account can not be registered if the user wasn't previously registered

# The database and tables creation (DDL)

```
conn = sqlite3.connect('bank.db')
cursor = conn.cursor()

try:
  cursor.execute("""CREATE TABLE IF NOT EXISTS tb_user(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cpf VARCHAR(11) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    birth DATE NOT NULL,
    address VARCHAR(255) NOT NULL
  ) """)

  cursor.execute("""CREATE TABLE IF NOT EXISTS tb_account(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agency VARCHAR(4) NOT NULL,
    balance FLOAT CHECK (balance >= 0),
    id_user INT NOT NULL,
    CONSTRAINT fk_id_user_user FOREIGN KEY (id_user)
    REFERENCES tb_user (id),
    CONSTRAINT uq_user_agency UNIQUE (agency,id_user)
  ) """)

  cursor.execute("""CREATE TABLE IF NOT EXISTS tb_typ_op(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type VARCHAR(20) NOT NULL
  ) """)

  cursor.execute("""CREATE TABLE IF NOT EXISTS tb_statement(
    id_account INT NOT NULL,
    id_operation INT NOT NULL,
    datetime DATETIME NOT NULL,
    value VARCHAR(15) NOT NULL,
    CONSTRAINT fk_id_account_statement
    FOREIGN KEY (id_account) REFERENCES tb_account (id),
    CONSTRAINT fk_id_operation_statement
    FOREIGN KEY (id_operation) REFERENCES tb_operation (id)
  ) """)
except Exception as e:
  print(f'Error: {e}')
else:
  conn.commit()
  print("Tables creation executed with success!")

try:
  cursor.execute("""INSERT INTO tb_typ_op VALUES
  (1,"Withdraw"),
  (2,"Deposit")
  """)
except Exception as e:
  print(f'Error: {e}')
else:
  conn.commit()
  print("Operation type register was executed with success!")
```

# The Python classes

```
class User:
    conn = sqlite3.connect('bank.db')
    cursor = conn.cursor()

    def __init__(self, cpf, name, birth, address):
        self.cpf = str(cpf).strip().replace('.', '').replace(' ', '').replace('-', '')
        self.name = name
        self.birth = birth
        self.address = address

        user_cpf = f"SELECT id FROM tb_user WHERE cpf = ?"
        query_user = User.cursor.execute(user_cpf, (self.cpf,)).fetchone()

        if query_user is None:
            try:
                User.cursor.execute(
                    f'''INSERT INTO tb_user (cpf, name, birth, address) VALUES
                    {(self.cpf, self.name, self.birth, self.address)}''')
            except Exception as e:
                print(f'Error: {e}')
            else:
                print('User registered with success!')
                User.conn.commit()
        else:
            print('User already registered')

    def id(self):
      id_query = f"SELECT id FROM tb_user WHERE cpf = {self.cpf}"
      id_query = User.cursor.execute(id_query).fetchone()[0]
      return id_query

class Account:
  conn = sqlite3.connect('bank.db')
  cursor = conn.cursor()

  cursor.execute("SELECT MAX(id) FROM tb_account")
  query = cursor.fetchone()
  if query[0] is None:
    id_account = 1
  else:
    id_account = query[0] + 1

  cursor.execute('SELECT balance FROM tb_account WHERE id = ?',(id_account,))
  query = cursor.fetchone()
  if query is None:
    balance = 0
  else:
    balance = query[0]

  def __init__(self, balance=balance, *, agency, id_user):
    self.balance = balance
    self.agency = agency
    self.id_user = id_user

    account_user = f"SELECT id FROM tb_user WHERE id = {self.id_user}"
    query_user = User.cursor.execute(account_user).fetchone()

    if query_user is not None:
      account_check_query = "SELECT id FROM tb_account WHERE id_user = ? AND agency = ?"
      query_account = self.cursor.execute(account_check_query, (self.id_user, self.agency)).fetchone()
      if query_account is None:
        try:
          self.cursor.execute('''INSERT INTO tb_account (agency, balance, id_user)
                              VALUES (?, ?, ?)''',(self.agency, self.balance, self.id_user))
        except Exception as e:
          print(f'Error: {e}')
        else:
          print('Account registered with success!')
          self.conn.commit()
      else:
        print('Account already registered')
    else:
      print('User does not exist in the database')

  def id(self):
    id_query = f"SELECT id FROM tb_account WHERE id_user = {self.id_user} AND agency = '{self.agency}'"
    id_query = User.cursor.execute(id_query).fetchone()[0]
    return id_query

  def withdraw(self,value=0):
    id_account = self.id()
    balance = f"SELECT balance FROM tb_account WHERE id = {id_account}"
    balance = cursor.execute(balance).fetchone()[0]
    try:
      if value > balance:
        print("Error: you can not withdraw a balance higher than current balance")
      elif round(value,2) > 0:
        balance -= value
        balance = round(balance,2)
        cursor.execute(f'UPDATE tb_account SET balance = {balance} WHERE id = {id_account}')
        cursor.execute(f"INSERT INTO tb_statement VALUES ({id_account},1,'{datetime.now()}','R$ {value:.2f}')")
        print("Withdraw occurred with success!")
    except Exception as e:
      print(f'Erro: {e}')
    else:
      conn.commit()

  def deposit(self,value=0):
    id_account = self.id()
    balance = f"SELECT balance FROM tb_account WHERE id = {id_account}"
    balance = cursor.execute(balance).fetchone()[0]
    try:
      if round(value,2) >= 0:
        balance += value
        balance = round(balance,2)
        cursor.execute(f'UPDATE tb_account SET balance = {balance} WHERE id = {self.id()}')
        cursor.execute(f"INSERT INTO tb_statement VALUES ({self.id()},2,'{datetime.now()}','R$ {value:.2f}')")
        print("Deposit occurred with success!")
      else:
        print("Error: you can not deposit zero or negative values")
    except Exception as e:
      print(f'Erro: {e}')
    else:
      conn.commit()

  def statement(self):
    id_account = self.id()
    query = f'''SELECT t.type AS Operation, s.value AS Value, s.datetime AS Datetime
            FROM tb_statement AS s
            INNER JOIN tb_account AS a
            ON a.id = s.id_account
            INNER JOIN tb_typ_op AS t
            ON t.id = s.id_operation
            WHERE a.id = {id_account}'''
    return pd.read_sql(query,conn)
```
