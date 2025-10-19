use query_core::{QueryError, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Select,
    From,
    Where,
    Group,
    Order,
    By,
    Having,
    Limit,
    Offset,
    Join,
    Inner,
    Left,
    Right,
    Full,
    Cross,
    Outer,
    On,
    As,
    And,
    Or,
    Not,
    In,
    Between,
    Like,
    Is,
    Null,
    Asc,
    Desc,
    Count,
    Sum,
    Avg,
    Min,
    Max,

    // Operators
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,

    // Delimiters
    LeftParen,
    RightParen,
    Comma,
    Dot,
    Semicolon,

    // Literals
    Number(String),
    String(String),
    Identifier(String),

    // Special
    Eof,
}

pub struct Lexer {
    input: Vec<char>,
    position: usize,
}

impl Lexer {
    pub fn new(input: &str) -> Self {
        Self {
            input: input.chars().collect(),
            position: 0,
        }
    }

    pub fn tokenize(&mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();

        while self.position < self.input.len() {
            self.skip_whitespace();

            if self.position >= self.input.len() {
                break;
            }

            let token = self.next_token()?;
            if token != Token::Eof {
                tokens.push(token);
            }
        }

        tokens.push(Token::Eof);
        Ok(tokens)
    }

    fn next_token(&mut self) -> Result<Token> {
        let ch = self.current_char();

        let token = match ch {
            '+' => {
                self.advance();
                Token::Plus
            }
            '-' => {
                self.advance();
                Token::Minus
            }
            '*' => {
                self.advance();
                Token::Star
            }
            '/' => {
                self.advance();
                Token::Slash
            }
            '%' => {
                self.advance();
                Token::Percent
            }
            '=' => {
                self.advance();
                Token::Equal
            }
            '<' => {
                self.advance();
                if self.current_char() == '=' {
                    self.advance();
                    Token::LessEqual
                } else if self.current_char() == '>' {
                    self.advance();
                    Token::NotEqual
                } else {
                    Token::Less
                }
            }
            '>' => {
                self.advance();
                if self.current_char() == '=' {
                    self.advance();
                    Token::GreaterEqual
                } else {
                    Token::Greater
                }
            }
            '!' => {
                self.advance();
                if self.current_char() == '=' {
                    self.advance();
                    Token::NotEqual
                } else {
                    return Err(QueryError::ParseError(
                        "Unexpected character '!'".to_string(),
                    ));
                }
            }
            '(' => {
                self.advance();
                Token::LeftParen
            }
            ')' => {
                self.advance();
                Token::RightParen
            }
            ',' => {
                self.advance();
                Token::Comma
            }
            '.' => {
                self.advance();
                Token::Dot
            }
            ';' => {
                self.advance();
                Token::Semicolon
            }
            '\'' | '"' => self.read_string()?,
            _ if ch.is_ascii_digit() => self.read_number()?,
            _ if ch.is_alphabetic() || ch == '_' => self.read_identifier()?,
            _ => {
                return Err(QueryError::ParseError(format!(
                    "Unexpected character: '{}'",
                    ch
                )));
            }
        };

        Ok(token)
    }

    fn read_string(&mut self) -> Result<Token> {
        let quote = self.current_char();
        self.advance();

        let mut value = String::new();
        while self.position < self.input.len() && self.current_char() != quote {
            value.push(self.current_char());
            self.advance();
        }

        if self.position >= self.input.len() {
            return Err(QueryError::ParseError("Unterminated string".to_string()));
        }

        self.advance(); // Skip closing quote
        Ok(Token::String(value))
    }

    fn read_number(&mut self) -> Result<Token> {
        let mut number = String::new();

        while self.position < self.input.len()
            && (self.current_char().is_ascii_digit() || self.current_char() == '.')
        {
            number.push(self.current_char());
            self.advance();
        }

        Ok(Token::Number(number))
    }

    fn read_identifier(&mut self) -> Result<Token> {
        let mut ident = String::new();

        while self.position < self.input.len()
            && (self.current_char().is_alphanumeric() || self.current_char() == '_')
        {
            ident.push(self.current_char());
            self.advance();
        }

        let token = match ident.to_uppercase().as_str() {
            "SELECT" => Token::Select,
            "FROM" => Token::From,
            "WHERE" => Token::Where,
            "GROUP" => Token::Group,
            "ORDER" => Token::Order,
            "BY" => Token::By,
            "HAVING" => Token::Having,
            "LIMIT" => Token::Limit,
            "OFFSET" => Token::Offset,
            "JOIN" => Token::Join,
            "INNER" => Token::Inner,
            "LEFT" => Token::Left,
            "RIGHT" => Token::Right,
            "FULL" => Token::Full,
            "CROSS" => Token::Cross,
            "OUTER" => Token::Outer,
            "ON" => Token::On,
            "AS" => Token::As,
            "AND" => Token::And,
            "OR" => Token::Or,
            "NOT" => Token::Not,
            "IN" => Token::In,
            "BETWEEN" => Token::Between,
            "LIKE" => Token::Like,
            "IS" => Token::Is,
            "NULL" => Token::Null,
            "ASC" => Token::Asc,
            "DESC" => Token::Desc,
            "COUNT" => Token::Count,
            "SUM" => Token::Sum,
            "AVG" => Token::Avg,
            "MIN" => Token::Min,
            "MAX" => Token::Max,
            _ => Token::Identifier(ident),
        };

        Ok(token)
    }

    fn current_char(&self) -> char {
        if self.position < self.input.len() {
            self.input[self.position]
        } else {
            '\0'
        }
    }

    fn advance(&mut self) {
        self.position += 1;
    }

    fn skip_whitespace(&mut self) {
        while self.position < self.input.len() && self.current_char().is_whitespace() {
            self.advance();
        }
    }
}
