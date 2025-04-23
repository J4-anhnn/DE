import unittest
from app import app, db
from app.models import User

class AppTestCase(unittest.TestCase):
    
    def setUp(self):
        app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:password@localhost:5432/sampledb_test'
        app.config['TESTING'] = True
        self.app = app.test_client()
        db.create_all()
    
    def tearDown(self):
        db.session.remove()
        db.drop_all()
    
    def test_index(self):
        response = self.app.get('/')
        self.assertEqual(response.data, b"Hello, World!")
    
    def test_add_user(self):
        response = self.app.post('/add_user', data={'username': 'testuser', 'email': 'test@example.com'})
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"User added successfully!", response.data)
    
    def test_get_users(self):
        self.app.post('/add_user', data={'username': 'testuser', 'email': 'test@example.com'})
        response = self.app.get('/users')
        self.assertIn(b'testuser', response.data)

if __name__ == '__main__':
    unittest.main()
