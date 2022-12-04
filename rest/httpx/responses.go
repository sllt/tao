package httpx

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/sllt/tao/core/logx"
	"github.com/sllt/tao/rest/internal/errcode"
	"github.com/sllt/tao/rest/internal/header"
)

var (
	errorHandler    func(error) (int, interface{})
	lock            sync.RWMutex
	errorHandlerCtx func(context.Context, error) (int, interface{})
)

// Error writes err into w.
func Error(w http.ResponseWriter, err error, fns ...func(w http.ResponseWriter, err error)) {
	lock.RLock()
	handler := errorHandler
	lock.RUnlock()

	if handler == nil {
		if len(fns) > 0 {
			fns[0](w, err)
		} else if errcode.IsGrpcError(err) {
			// don't unwrap error and get status.Message(),
			// it hides the rpc error headers.
			http.Error(w, err.Error(), errcode.CodeFromGrpcError(err))
		} else {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}

		return
	}

	code, body := handler(err)
	if body == nil {
		w.WriteHeader(code)
		return
	}

	e, ok := body.(error)
	if ok {
		http.Error(w, e.Error(), code)
	} else {
		WriteJson(w, code, body)
	}
}

// Ok writes HTTP 200 OK into w.
func Ok(w http.ResponseWriter) {
	w.WriteHeader(http.StatusOK)
}

// OkJson writes v into w with 200 OK.
func OkJson(w http.ResponseWriter, v interface{}) {
	WriteJson(w, http.StatusOK, v)
}

// SetErrorHandler sets the error handler, which is called on calling Error.
func SetErrorHandler(handler func(error) (int, interface{})) {
	lock.Lock()
	defer lock.Unlock()
	errorHandler = handler
}

// WriteJson writes v as json string into w with code.
func WriteJson(w http.ResponseWriter, code int, v interface{}) {
	bs, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set(ContentType, header.JsonContentType)
	w.WriteHeader(code)

	if n, err := w.Write(bs); err != nil {
		// http.ErrHandlerTimeout has been handled by http.TimeoutHandler,
		// so it's ignored here.
		if err != http.ErrHandlerTimeout {
			logx.Errorf("write response failed, error: %s", err)
		}
	} else if n < len(bs) {
		logx.Errorf("actual bytes: %d, written bytes: %d", len(bs), n)
	}
}

// Error writes err into w.
func ErrorCtx(ctx context.Context, w http.ResponseWriter, err error, fns ...func(w http.ResponseWriter, err error)) {
	lock.RLock()
	handlerCtx := errorHandlerCtx
	lock.RUnlock()

	if handlerCtx == nil {
		if len(fns) > 0 {
			fns[0](w, err)
		} else if errcode.IsGrpcError(err) {
			// don't unwrap error and get status.Message(),
			// it hides the rpc error headers.
			http.Error(w, err.Error(), errcode.CodeFromGrpcError(err))
		} else {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}

		return
	}

	code, body := handlerCtx(ctx, err)
	if body == nil {
		w.WriteHeader(code)
		return
	}

	e, ok := body.(error)
	if ok {
		http.Error(w, e.Error(), code)
	} else {
		WriteJsonCtx(ctx, w, code, body)
	}
}

// OkJson writes v into w with 200 OK.
func OkJsonCtx(ctx context.Context, w http.ResponseWriter, v interface{}) {
	WriteJsonCtx(ctx, w, http.StatusOK, v)
}

// WriteJson writes v as json string into w with code.
func WriteJsonCtx(ctx context.Context, w http.ResponseWriter, code int, v interface{}) {
	bs, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set(ContentType, header.JsonContentType)
	w.WriteHeader(code)

	if n, err := w.Write(bs); err != nil {
		// http.ErrHandlerTimeout has been handled by http.TimeoutHandler,
		// so it's ignored here.
		if err != http.ErrHandlerTimeout {
			logx.WithContext(ctx).Errorf("write response failed, error: %s", err)
		}
	} else if n < len(bs) {
		logx.WithContext(ctx).Errorf("actual bytes: %d, written bytes: %d", len(bs), n)
	}
}

// SetErrorHandler sets the error handler, which is called on calling Error.
func SetErrorHandlerCtx(handlerCtx func(context.Context, error) (int, interface{})) {
	lock.Lock()
	defer lock.Unlock()
	errorHandlerCtx = handlerCtx
}
