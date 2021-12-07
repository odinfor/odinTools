package autoBindWrap

import (
	"github.com/gin-gonic/gin"
	"net/http"
	"reflect"
)

//
// AutoBindWrap
// @Description: 自动绑定参数的包装方法减少bind参数垄余代码,例如: func NeedBindGet(c *gin.content, params *GetParams)
// @param ctrFunc 签名函数
// @return gin.HandlerFunc
//
func AutoBindWrap(handlerFunc interface{}) gin.HandlerFunc {
	return func(context *gin.Context) {
		// 获取handler func函数参数struct,创建参数实例
		handlerFuncType := reflect.TypeOf(handlerFunc)
		handlerFuncValue := reflect.ValueOf(handlerFunc)

		// 检查参数类型,第一个
		if handlerFuncType.Kind() != reflect.Func {
			panic("not support type, handler must be a func type!")
			return
		}
		numIn := handlerFuncType.NumIn()
		if numIn != 2 {
			panic("not support params len. must need two params!")
			return
		}

		// bind参数
		handlerFuncParams := make([]reflect.Value, numIn)
		for i := 0; i < numIn; i++ {
			pt := handlerFuncType.In(i)
			// handle gin.content
			if pt == reflect.TypeOf(&gin.Context{}) {
				handlerFuncParams[i] = reflect.ValueOf(context)
				continue
			}
			// handle params, 根据请求方法bind
			if pt.Kind() == reflect.Ptr && pt.Elem().Kind() == reflect.Struct {
				pv := reflect.New(pt.Elem()).Interface()
				var err error
				switch context.Request.Method {
				case http.MethodGet:
					err = context.ShouldBindQuery(pv)
				default:
					err = context.ShouldBindJSON(pv)
				}
				if err != nil {
					//panic(fmt.Errorf("bind params error: %v", err))
					context.JSON(http.StatusOK, gin.H{
						"code": 200,
						"message": "fail",
						"data": "绑定参数错误",
					})
					return
				}
				handlerFuncParams[i] = reflect.ValueOf(pv)
			}
		}
		// 调用真实方法
		handlerFuncValue.Call(handlerFuncParams)
	}
}