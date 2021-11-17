package jwtCli

import (
	"errors"
	"fmt"
	"github.com/dgrijalva/jwt-go"
	"time"
)

type registerJWT struct {
	SigningKey []byte
}

const jwtKey = "framework-devops-registery:KeyForGenerateToken"

func NewJWT() *registerJWT {
	return &registerJWT{[]byte(jwtKey)}
}

// 自定义有效载荷(这里采用自定义的UserName和Password作为有效载荷的一部分)
type CustomClaims struct {
	UserName string `json:"username"`
	// StandardClaims结构体实现了Claims接口(Valid()函数)
	jwt.StandardClaims
}

// token 生成,使用jwt-go库生成token,指定编码的算法为jwt.SigningMethodHS256
func (j *registerJWT) GenerateToken(username string) (string, error) {
	// 构造用户claims信息
	claims := CustomClaims{
		username,
		jwt.StandardClaims{
			NotBefore: int64(time.Now().Unix() - 1000),
			ExpiresAt: int64(time.Now().Unix() + 3600),
			Issuer:    jwtKey,
		},
	}

	// 根据claims生成token对象
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	if token, err := t.SignedString(j.SigningKey); err != nil {
		return "", err
	} else {
		return token, nil
	}
}

// token解码
func (j *registerJWT) ParserToken(tokenString string) (*CustomClaims, error) {
	// https://gowalker.org/github.com/dgrijalva/jwt-go#ParseWithClaims
	// 输入用户自定义的Claims结构体对象,token,以及自定义函数解析token字符串为jwt的token结构体指针
	token, err := jwt.ParseWithClaims(tokenString, &CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
		return j.SigningKey, nil
	})

	if err != nil {
		// https://gowalker.org/github.com/dgrijalva/jwt-go#ValidationError
		// jwtCli.ValidationError 是一个无效token的错误结构
		if ve, ok := err.(*jwt.ValidationError); ok {
			if ve.Errors&jwt.ValidationErrorMalformed != 0 {
				return nil, j.MalformedTokenError()
			} else if ve.Errors&jwt.ValidationErrorExpired != 0 {
				return nil, j.ExpiredTokenError()
			} else if ve.Errors&jwt.ValidationErrorNotValidYet != 0 {
				return nil, j.MalformedTokenError()
			} else {
				return nil, j.MalformedTokenError()
			}
		}
	}

	// 将token中的claims信息解析出来并端游成用户自定义的有效载荷结构
	if token == nil {
		return nil, fmt.Errorf("toekn错误")
	}
	if claims, ok := token.Claims.(*CustomClaims); ok && token.Valid {
		return claims, nil
	}

	return nil, fmt.Errorf("无效的token")
}

func (j *registerJWT) InvalidTokenError() error {
	return errors.New("无效的token")
}

func (j *registerJWT) ExpiredTokenError() error {
	return errors.New("token过期")
}

func (j *registerJWT) MalformedTokenError() error {
	return errors.New("token不可用")
}
