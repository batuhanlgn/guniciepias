# auth.py - Authentication System
import hashlib
import json
import os
from datetime import datetime

USERS_FILE = "users.json"

def hash_password(password):
    """Hash password with salt"""
    salt = "gip_dashboard_2025"
    return hashlib.sha256((password + salt).encode()).hexdigest()

def load_users():
    """Load users from JSON file"""
    if not os.path.exists(USERS_FILE):
        # Create default admin user
        default_users = {
            "admin": {
                "password": hash_password("admin123"),
                "role": "admin",
                "approved": True,
                "created_at": datetime.now().isoformat(),
                "permissions": {
                    "visual_alarms": True,
                    "sound_alarms": True,
                    "telegram": True,
                    "sms": True,
                    "websocket": True,
                    "user_management": True
                }
            }
        }
        save_users(default_users)
        return default_users
    
    try:
        with open(USERS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return {}

def save_users(users):
    """Save users to JSON file"""
    with open(USERS_FILE, 'w', encoding='utf-8') as f:
        json.dump(users, f, indent=2, ensure_ascii=False)

def authenticate_user(username, password):
    """Authenticate user login"""
    users = load_users()
    if username not in users:
        return False, "Kullanıcı bulunamadı"
    
    user = users[username]
    if not user.get("approved", False):
        return False, "Hesabınız henüz onaylanmamış"
    
    if user["password"] == hash_password(password):
        return True, "Giriş başarılı"
    else:
        return False, "Yanlış şifre"

def register_user(username, password):
    """Register new user"""
    users = load_users()
    
    if username in users:
        return False, "Bu kullanıcı adı zaten mevcut"
    
    if len(username) < 3:
        return False, "Kullanıcı adı en az 3 karakter olmalı"
    
    if len(password) < 6:
        return False, "Şifre en az 6 karakter olmalı"
    
    # Add new user with pending approval
    users[username] = {
        "password": hash_password(password),
        "role": "user",
        "approved": False,
        "created_at": datetime.now().isoformat(),
        "permissions": {
            "visual_alarms": True,
            "sound_alarms": True,
            "telegram": False,
            "sms": False,
            "websocket": False,
            "user_management": False
        }
    }
    
    save_users(users)
    return True, "Kayıt başarılı! Onay bekleniyor."

def get_user_permissions(username):
    """Get user permissions"""
    users = load_users()
    if username not in users:
        return {}
    return users[username].get("permissions", {})

def create_user_by_admin(admin_username, new_username, password, user_role="user"):
    """Admin tarafından doğrudan kullanıcı oluşturma"""
    try:
        users = load_users()
        
        # Check if admin
        admin_user = users.get(admin_username, {})
        if admin_user.get('role') != 'admin':
            return False, "Bu işlem için admin yetkisi gerekli"
        
        # Check if username already exists
        if new_username in users:
            return False, "Bu kullanıcı adı zaten mevcut"
        
        # Validate inputs
        if len(new_username) < 3:
            return False, "Kullanıcı adı en az 3 karakter olmalı"
        
        if len(password) < 6:
            return False, "Şifre en az 6 karakter olmalı"
        
        # Set permissions based on role
        if user_role == "admin":
            permissions = {
                "visual_alarms": True,
                "sound_alarms": True,
                "telegram": True,
                "sms": True,
                "websocket": True,
                "user_management": True
            }
        else:
            permissions = {
                "visual_alarms": True,
                "sound_alarms": True,
                "telegram": False,
                "sms": False,
                "websocket": False,
                "user_management": False
            }
        
        # Create new user
        users[new_username] = {
            'password': hash_password(password),
            'role': user_role,
            'approved': True,  # Admin tarafından oluşturulan kullanıcılar otomatik onaylı
            'created_at': datetime.now().isoformat(),
            'created_by': admin_username,
            'permissions': permissions
        }
        
        # Save users
        save_users(users)
        
        role_text = "Admin" if user_role == "admin" else "Kullanıcı"
        return True, f"{role_text} '{new_username}' başarıyla oluşturuldu"
        
    except Exception as e:
        print(f"User creation error: {e}")
        return False, "Kullanıcı oluşturma hatası"

def approve_user(admin_username, target_username):
    """Approve pending user (admin only)"""
    users = load_users()
    
    # Check if admin
    if users.get(admin_username, {}).get("role") != "admin":
        return False, "Yetkiniz yok"
    
    if target_username not in users:
        return False, "Kullanıcı bulunamadı"
    
    users[target_username]["approved"] = True
    save_users(users)
    return True, f"{target_username} onaylandı"

def get_pending_users():
    """Get list of pending users"""
    users = load_users()
    pending = []
    for username, user in users.items():
        if not user.get("approved", False) and user.get("role") == "user":
            pending.append({
                "username": username,
                "created_at": user.get("created_at", ""),
                "role": user.get("role", "user")
            })
    return pending
